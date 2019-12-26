package me.ele.jarch.athena.scheduler;

import me.ele.jarch.athena.SQLLogFilter;
import me.ele.jarch.athena.constant.Metrics;
import me.ele.jarch.athena.constant.TraceNames;
import me.ele.jarch.athena.netty.AthenaServer;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.netty.state.SESSION_STATUS;
import me.ele.jarch.athena.server.pool.ServerSession;
import me.ele.jarch.athena.sql.CmdQuery;
import me.ele.jarch.athena.sql.QUERY_TYPE;
import me.ele.jarch.athena.util.QPSCounter;
import me.ele.jarch.athena.util.QueryStatistics;
import me.ele.jarch.athena.util.etrace.MetricFactory;
import me.ele.jarch.athena.util.etrace.MetricMetaExtractor;
import me.ele.jarch.athena.worker.SchedulerWorker;
import me.ele.jarch.athena.worker.manager.AutoKillerManager;
import me.ele.jarch.athena.worker.manager.AutoKillerManager.KillerStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

//autokill_open 开启或关闭这个功能,off表示关闭,on表示开启,默认为on
//autokill_slow_sql 慢sql的毫秒阀值,默认是30
//autokill_slow_commit 慢commit的毫秒阀值,默认是3000
//autokill_lower_size 触发AutoKiller的高水位,默认是max_active_db_sessions*10
//autokill_upper_size 停止AutoKiller的低水位,默认是max_active_db_sessions/2
public class AutoKiller implements Delayed {
    private static final Logger logger = LoggerFactory.getLogger(AutoKiller.class);
    private static final int SCAN_INTERVAL_MILLIS = 20;

    private static DelayQueue<AutoKiller> delayQueue = new DelayQueue<>();
    private static ConcurrentHashMap<IdDecorator<Scheduler>, Object> schedulersInAutoKiller =
        new ConcurrentHashMap<>();

    private final Scheduler sched;

    private final long startTimeInMillis = System.currentTimeMillis();
    private long nextTimeInMillis = startTimeInMillis + SCAN_INTERVAL_MILLIS;

    private int killedCnt = 0;
    private WindowCounter smokeCounter =
        new WindowCounter(AthenaServer.globalZKCache.getSmokeWindowMillis() / SCAN_INTERVAL_MILLIS);


    private static class AutoKillerThread extends Thread {
        public AutoKillerThread() {
            this.setName(AutoKiller.class.getName());
        }

        @Override public void run() {
            while (true) {
                try {
                    AutoKiller autoKiller = delayQueue.take();
                    autoKiller.scanSlowSqlAndTran();
                } catch (Exception t) {
                    logger.error("", t);
                }
            }

        }
    }


    static {
        new AutoKillerThread().start();
    }

    private AutoKiller(Scheduler sched) {
        Objects.requireNonNull(sched);
        this.sched = sched;
        logger.error(String.format("AutoKiller Job[%s] started ... ", sched.getName()));
    }

    @SuppressWarnings("unchecked") public static void startAutoKiller(Scheduler s) {
        if (schedulersInAutoKiller.putIfAbsent(s.ID_DECORATOR, new Object()) == null) {
            delayQueue.offer(new AutoKiller(s));
        }
    }

    public static boolean isSchedulerInAutoKill(Scheduler scheduler) {
        return schedulersInAutoKiller.containsKey(scheduler.ID_DECORATOR);
    }

    public void scanSlowSqlAndTran() {
        boolean runAgain = false;
        try {
            doRun();
            if (sched.getZKCache().isAutoKillerOpen() && sched.getQueueBlockingTaskCount() > sched
                .getZKCache().getSmartLowerAutoKillerSize()) {
                this.setNextTimeInMillis();
                delayQueue.offer(this);
                runAgain = true;
            } else {
                schedulersInAutoKiller.remove(sched.ID_DECORATOR);
                long duration = (System.currentTimeMillis() - startTimeInMillis + 500) / 1000;
                String msg = String
                    .format("AutoKiller Job[%s] ended, total killed %d, duration: %d seconds",
                        sched.getName(), killedCnt, duration);
                logger.error(msg);
            }
        } finally {
            if (!runAgain) {
                schedulersInAutoKiller.remove(sched.ID_DECORATOR);
            }

        }
    }

    private void doRun() {
        int[] thisRoundKill = {0};
        HangSessionMonitor.getAllCtx().stream().filter(x -> x != null).forEach(x -> {
            long now = System.currentTimeMillis();
            boolean slowTrans = isSlowTrans(x, now);
            boolean slowSQL = isSlowSQL(x, now);

            if (slowSQL || slowTrans) {

                if (slowSQL) {
                    SchedulerWorker.getInstance()
                        .enqueue(new AutoKillerManager(x, KillerStatus.KILL_SQL));
                } else {
                    SchedulerWorker.getInstance()
                        .enqueue(new AutoKillerManager(x, KillerStatus.KILL_TRANS));
                }
                killedCnt++;
                thisRoundKill[0]++;
            }

            if (slowSQL) {
                logger.warn("Kill one Ctx[slowSQL], please see error.log for details");
                SQLLogFilter.error(String.format("Kill this Ctx[slowSQL]: %s", x.toString()));
                MetricFactory.newCounter(Metrics.AK_KILL_SQL).addTag(TraceNames.DALGROUP,
                    MetricMetaExtractor.extractDALGroupName(this.sched))
                    .addTag(TraceNames.DBID, MetricMetaExtractor.extractDBid(this.sched)).once();
            } else if (slowTrans) {
                logger.warn("Kill one Ctx[slowTran], please see error.log for details");
                SQLLogFilter.error(String.format("Kill this Ctx[slowTran]: %s", x.toString()));
                MetricFactory.newCounter(Metrics.AK_KILL_TRAN).addTag(TraceNames.DALGROUP,
                    MetricMetaExtractor.extractDALGroupName(this.sched))
                    .addTag(TraceNames.DBID, MetricMetaExtractor.extractDBid(this.sched)).once();
            }
        });
        smokeCounter.record(thisRoundKill[0]);
        sickIfExceedThreshold();
        logger.error(String.format("AutoKiller Job kill %d ctx in this round", thisRoundKill[0]));
    }


    private boolean isSlowTrans(SqlSessionContext x, long now) {
        if (!x.isInTransStatus() || x.getStatus().equals(SESSION_STATUS.QUIT)) {
            // server session is empty means this is the first DML in the transaction that has not sent to the server yet. This will block nothing at this
            // point, do not kill this transacaction.
            return false;
        }

        ServerSession serverSession = x.shardedSession.get(sched.getInfo().getGroup());
        if (serverSession == null || !serverSession.getActiveDBId()
            .equals(sched.getInfo().getId())) {
            // The sqlSessionContext did not send any trans query to this DB
            return false;
        }

        if (x.sqlSessionContextUtil.getTransStatistics().gettEndTime() != 0) {
            // The transaction is finished
            return false;
        }

        QueryStatistics queryStatistics =
            x.sqlSessionContextUtil.getTransStatistics().getlastQueryStatistics();
        if (queryStatistics.getcSendTime() == 0) {
            // The last query of this transaction is ongoing
            return false;
        }
        if (now - queryStatistics.getcSendTime() > sched.getZKCache().getAutoKillerSlowCommit()) {
            return true;
        }
        return false;
    }

    private boolean isInQuery(SqlSessionContext x) {
        CmdQuery curCmdQuery = x.curCmdQuery;
        if (curCmdQuery == null || "".equals(curCmdQuery.query) || x.getStatus()
            .equals(SESSION_STATUS.QUIT)) {
            return false;
        }
        if (isPassedQueryType(curCmdQuery.queryType)) {
            return false;
        }
        QueryStatistics queryStatistics = x.sqlSessionContextUtil.getCurrentQueryStatistics();
        if (queryStatistics.getsSendTime() == 0) {
            // The query not sent yet.
            return false;
        }
        if (queryStatistics.getsRecvTime() != 0) {
            // Dal is receiving query response.
            return false;
        }
        if (x.getStatus() == SESSION_STATUS.QUERY_RESULT
            || x.getStatus() == SESSION_STATUS.GRAY_RESULT) {
            return true;
        }
        return false;
    }

    private boolean isPassedQueryType(QUERY_TYPE queryType) {
        switch (queryType) {
            case COMMIT:
            case ROLLBACK:
            case INSERT:
            case DELETE:
            case UPDATE:
                return true;
            default:
                return false;
        }
    }

    private boolean isSlowSQL(SqlSessionContext x, long now) {
        if (sched != x.scheduler) {
            return false;
        }
        if (!isInQuery(x)) {
            return false;
        }
        if (now > x.sqlSessionContextUtil.getCurrentQueryStatistics().getsSendTime() + sched
            .getZKCache().getSmartAutoKillerSlowSQL(sched.getQueueBlockingTaskCount())) {
            return true;
        }
        return false;
    }

    @Override public long getDelay(TimeUnit unit) {
        return unit.convert(nextTimeInMillis - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    @Override public int compareTo(Delayed o) {
        return Long
            .compare(this.getDelay(TimeUnit.MILLISECONDS), o.getDelay(TimeUnit.MILLISECONDS));
    }

    public void setNextTimeInMillis() {
        this.nextTimeInMillis = System.currentTimeMillis() + SCAN_INTERVAL_MILLIS;
    }

    private void sickIfExceedThreshold() {
        if (shouldSick() && !DBChannelDispatcher.getHolders().get(sched.getChannelName())
            .getSickMonitor().isSicked(sched.getInfo().getQualifiedDbId())) {
            logger.error(String.format("db: %s auto sicked", sched.getInfo().getQualifiedDbId()));
            DBChannelDispatcher.getHolders().get(sched.getChannelName()).getSickMonitor()
                .sick(sched.getInfo().getQualifiedDbId());
            AthenaServer.commonJobScheduler
                .addOneTimeJob("recover.sick." + sched.getInfo().getQualifiedDbId(),
                    AthenaServer.globalZKCache.getFuseWindowMillis(), () -> recoverSick());
        }
    }

    private void recoverSick() {
        if (DBChannelDispatcher.getHolders().get(sched.getChannelName()).getSickMonitor()
            .tryRecoverAutoSick(sched.getInfo().getQualifiedDbId())) {
            logger.error(
                String.format("db: %s auto sick recover", sched.getInfo().getQualifiedDbId()));
        }
    }

    private boolean shouldSick() {
        if (smokeCounter.getLastIndex()
            < AthenaServer.globalZKCache.getSmokeWindowMillis() / SCAN_INTERVAL_MILLIS) {
            return false;
        }
        int killedSum = smokeCounter.getHistory();
        int passedSum = QPSCounter.getInstance()
            .getQPS(sched.getDbQueueId(), AthenaServer.globalZKCache.getSmokeWindowMillis());
        if (logger.isDebugEnabled()) {
            logger.debug(String
                .format("scheduler: %s, round: %d, killed: %d, passed: %d", sched.getName(),
                    smokeCounter.getLastIndex(), killedSum, passedSum));
        }
        if (killedSum > 0 && passedSum > 0) {
            return (passedSum / killedSum) <= AthenaServer.globalZKCache.getFusePassRate();
        }
        return false;
    }


}
