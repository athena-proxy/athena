package me.ele.jarch.athena.scheduler;

import com.github.mpjct.jmpjct.util.ErrorCode;
import me.ele.jarch.athena.allinone.DBConnectionInfo;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.constant.Metrics;
import me.ele.jarch.athena.netty.AthenaServer;
import me.ele.jarch.athena.netty.Monitorable;
import me.ele.jarch.athena.netty.SessionQuitTracer;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.scheduler.MulScheduler.PayloadType;
import me.ele.jarch.athena.sql.rscache.QueryResultCache;
import me.ele.jarch.athena.util.Job;
import me.ele.jarch.athena.util.ZKCache;
import me.ele.jarch.athena.util.etrace.MetricFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class Scheduler {
    private static final Logger LOGGER = LoggerFactory.getLogger(Scheduler.class);
    @SuppressWarnings({"unchecked", "rawtypes"}) public final IdDecorator ID_DECORATOR =
        new IdDecorator(this);
    final private AtomicLong seqGen = new AtomicLong(0);
    static final int MEDIUM_PRIORITY = 5;
    static final int HIGH_PRIORITY = 4;
    static final int URGENT_PRIORITY = 3;
    private final int dbQueueId;
    private final int trxQueueId;
    private final String name;

    private volatile double maxActiveDBSessions;
    private volatile double maxActiveDBTrx;

    private final List<Job> jobs = new ArrayList<>();

    /**
     * query results cached for sql
     */
    public final QueryResultCache queryResultCache = new QueryResultCache();

    private final DBConnectionInfo info;

    private String prefixName = "emptydbinfo";

    public DBConnectionInfo getInfo() {
        return info;
    }

    private final String channelName;

    public String getChannelName() {
        return channelName;
    }

    private ZKCache zkCache;

    public ZKCache getZKCache() {
        return zkCache;
    }

    public int getSemaphoreAvailablePermits() {
        return MulScheduler.getInstance().availablePermits(this.dbQueueId);
    }

    public int getQueueBlockingTaskCount() {
        return MulScheduler.getInstance().queueSize(this.dbQueueId) + MulScheduler.getInstance()
            .queueSize(this.trxQueueId);
    }

    public Scheduler(DBConnectionInfo info, ZKCache zkCache, String channelName) {
        this.zkCache = zkCache;
        this.channelName = channelName;
        maxActiveDBSessions = this.zkCache.getMaxActiveDbSessions();
        maxActiveDBTrx = this.zkCache.getMaxActiveDBTrx();
        this.info = info;
        if (info != null) {
            prefixName = String.format("%s.%s", info.getGroup(), info.getId());
        }
        this.name = prefixName;
        if (info != null) {
            this.dbQueueId = MulScheduler.getInstance()
                .register(this, this.channelName + ":" + info.getQualifiedDbId(),
                    floorInt(maxActiveDBSessions), thousandthDecimal(maxActiveDBSessions));
            this.trxQueueId = MulScheduler.getInstance()
                .register(this, this.channelName + ":" + info.getQualifiedDbId() + "_trx",
                    floorInt(maxActiveDBTrx), thousandthDecimal(maxActiveDBTrx));
        } else {
            this.dbQueueId = -1;
            this.trxQueueId = -1;
        }
    }

    public void start() {
        Monitorable schedulerMonitorJob = new SchedulerMonitorJob(this);

        Job job = AthenaServer.commonJobScheduler
            .addJob(prefixName + ".monitor_queue_size", 2 * 1000, schedulerMonitorJob::monitor);
        jobs.add(job);
    }

    public boolean enqueue(SqlSessionContext sqlCtx, PayloadType payloadType, boolean isBeginTrx) {
        if (MulScheduler.getInstance().queueSize(dbQueueId) + MulScheduler.getInstance()
            .queueSize(trxQueueId) >= zkCache.getMaxQueueSize()) {
            MetricFactory.newCounterWithScheduler(Metrics.SICK_KILL, this).once();
            LOGGER.warn(
                String.format("max queue size exceeded, close channel: %s", sqlCtx.toString()));
            sqlCtx.quitTracer.reportQuit(SessionQuitTracer.QuitTrace.DBFuse);
            sqlCtx.kill(ErrorCode.DALSICK, "sql sick \'" + this.info.getQualifiedDbId() + "\'");
            return false;
        }
        if (payloadType == PayloadType.TRX) {
            return MulScheduler.getInstance()
                .enqueue(sqlCtx, trxQueueId, payloadType, seqGen.incrementAndGet(), MEDIUM_PRIORITY,
                    false);
        } else {
            boolean isOverdraw = sqlCtx.isInTransStatus() && !isBeginTrx;
            int priority = MEDIUM_PRIORITY;
            if (isOverdraw) {
                priority = sqlCtx.getQueryType().isEndTrans() ? URGENT_PRIORITY : HIGH_PRIORITY;
            }
            return MulScheduler.getInstance()
                .enqueue(sqlCtx, dbQueueId, payloadType, seqGen.incrementAndGet(), priority,
                    isOverdraw);
        }
    }

    public synchronized void setMaxActiveDBSessions(double maxActiveDBSessions) {
        maxActiveDBSessions = maxActiveDBSessions > Constants.DUMMY_SEMAPHORE_MAX_COUNT ?
            Constants.DUMMY_SEMAPHORE_MAX_COUNT :
            maxActiveDBSessions;
        maxActiveDBSessions = maxActiveDBSessions <= 0 ? 0 : maxActiveDBSessions;

        double delta = maxActiveDBSessions - this.maxActiveDBSessions;
        if (delta == 0.0) {
            return;
        }
        int floorDelta = floorDiff(maxActiveDBSessions, this.maxActiveDBSessions);
        int decimalDelta = thousandthDecimalDiff(maxActiveDBSessions, this.maxActiveDBSessions);
        MulScheduler.getInstance().changeMaxSemaphore(this.dbQueueId, floorDelta, decimalDelta);
        this.maxActiveDBSessions = maxActiveDBSessions;
    }

    private static int floorInt(double value) {
        return (int) Math.floor(value);
    }

    private static int floorDiff(double newValue, double oldValue) {
        return floorInt(newValue) - floorInt(oldValue);
    }

    private static int thousandthDecimal(double value) {
        return (int) ((value - Math.floor(value)) * 1000);
    }

    /**
     * 小数部分差值的1000倍
     *
     * @param newValue
     * @param oldValue
     * @return
     */
    private static int thousandthDecimalDiff(double newValue, double oldValue) {
        return thousandthDecimal(newValue) - thousandthDecimal(oldValue);
    }

    public synchronized void setMaxActiveDBTrx(double maxActiveDBTrx) {
        maxActiveDBTrx = maxActiveDBTrx > Constants.DUMMY_SEMAPHORE_MAX_COUNT ?
            Constants.DUMMY_SEMAPHORE_MAX_COUNT :
            maxActiveDBTrx;
        maxActiveDBTrx = maxActiveDBTrx <= 1 ? 0 : maxActiveDBTrx;

        double delta = maxActiveDBTrx - this.maxActiveDBTrx;
        if (delta == 0.0) {
            return;
        }
        int floorDelta = floorDiff(maxActiveDBTrx, this.maxActiveDBTrx);
        int decimalDelta = thousandthDecimalDiff(maxActiveDBTrx, this.maxActiveDBTrx);
        MulScheduler.getInstance().changeMaxSemaphore(this.trxQueueId, floorDelta, decimalDelta);
        this.maxActiveDBTrx = maxActiveDBTrx;
    }

    public synchronized double getMaxActiveDBSessions() {
        return this.maxActiveDBSessions;
    }

    private void shutdown() {
        MulScheduler.getInstance().unregister(this.dbQueueId, this);
        MulScheduler.getInstance().unregister(this.trxQueueId, this);
        LOGGER.warn(prefixName + " is shutdown!");
        this.jobs.forEach(job -> {
            job.cancel();
        });
    }

    public void close() {
        Job job = AthenaServer.commonJobScheduler
            .addFirstDelayJob(prefixName + ".shutdown-scheduler", 10 * 1000, () -> {
                if (this.getQueueBlockingTaskCount() == 0) {
                    Scheduler.this.shutdown();
                }
            });
        this.jobs.add(job);
    }

    public String getName() {
        return name;
    }

    public int getDbQueueId() {
        return dbQueueId;
    }

    public int getTrxQueueId() {
        return trxQueueId;
    }
}
