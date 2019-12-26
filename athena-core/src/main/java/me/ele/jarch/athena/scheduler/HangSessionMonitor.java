package me.ele.jarch.athena.scheduler;

import me.ele.jarch.athena.SQLLogFilter;
import me.ele.jarch.athena.constant.Metrics;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.server.pool.ServerSession;
import me.ele.jarch.athena.util.NoThrow;
import me.ele.jarch.athena.util.TransStatistics;
import me.ele.jarch.athena.util.etrace.MetricFactory;
import me.ele.jarch.athena.worker.SchedulerWorker;
import me.ele.jarch.athena.worker.manager.SickManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Predicate;

public class HangSessionMonitor {
    private static final Logger logger = LoggerFactory.getLogger(HangSessionMonitor.class);
    private static final HangSessionMonitor MONITOR = new HangSessionMonitor();
    private Set<SqlSessionContext> totalCtxSet = new ConcurrentSkipListSet<>();
    public static final long LONG_LIVED_SESSION_LIMIT = 300 * 1000;

    public void addCtx(SqlSessionContext ctx) {
        totalCtxSet.add(ctx);
    }

    public void removeCtx(SqlSessionContext ctx) {
        totalCtxSet.remove(ctx);
    }

    public static HangSessionMonitor getInst() {
        return MONITOR;
    }

    public static Set<SqlSessionContext> getAllCtx() {
        return MONITOR.totalCtxSet;
    }

    private Map<Long, SqlSessionContext> prevCommits = new TreeMap<>();
    private Map<Long, SqlSessionContext> prevSqls = new TreeMap<>();

    public void scanHangSessions() {
        final long HEALTH_CHECK_OVERDUE = System.currentTimeMillis() - 2000;
        final long LONG_LIVED_SESSION_TIME = LONG_LIVED_SESSION_LIMIT;
        Map<Long, SqlSessionContext> curCommits = new TreeMap<>();
        Map<Long, SqlSessionContext> curSqls = new TreeMap<>();
        totalCtxSet.stream().forEach(x -> {
            if (x.commitId != -1)
                curCommits.put(x.commitId, x);
            if (x.queryId != -1)
                curSqls.put(x.queryId, x);

            if (x.authenticator.isDalHealthCheckUser
                && x.authenticator.birthdayInMill < HEALTH_CHECK_OVERDUE) {
                NoThrow.call(() -> {
                    x.doQuit();
                    if (logger.isDebugEnabled()) {
                        logger.debug(String
                            .format("hang_heartbeat=[user:%s,connecting time:%d,client IP:%s",
                                x.authenticator.userName, x.authenticator.birthdayInMill,
                                x.getClientAddr()));
                    }
                });
            }

            if (!x.authenticator.isDalHealthCheckUser
                && x.sqlSessionContextUtil.getLastSQLTimeInMills()
                > x.authenticator.birthdayInMill + LONG_LIVED_SESSION_TIME) {
                NoThrow.call(() -> {
                    if (x.longLivedSession)
                        return;
                    MetricFactory.newCounterWithSqlSessionContext(Metrics.LONG_LIVED_SESSION, x)
                        .once();
                    SQLLogFilter.error(String
                        .format("long_lived_session= org:%s, dal_group:%s, user:%s, client:%s",
                            x.getHolder().getDalGroup().getOrg(),
                            x.getHolder().getDalGroup().getName(), x.authenticator.userName,
                            x.getClientInfo()));
                    x.longLivedSession = true;
                });
            }
        });
        prevCommits.forEach((k, v) -> {
            if (v.commitId != -1 && curCommits.containsKey(k)) {
                TransStatistics ts = v.sqlSessionContextUtil.getTransStatistics();
                MetricFactory.newCounterWithSqlSessionContext(Metrics.HANG_COMMIT, v).once();
                SQLLogFilter.error("hang_commit=" + v.toString().replace('\n', ' '),
                    Collections.singletonMap("rid", ts.getRid()));
                // prevent log same commit twice
                v.commitId = -1;
            }
        });
        prevCommits = curCommits;

        prevSqls.forEach((k, v) -> {
            if (v.queryId != -1 && curSqls.containsKey(k)) {
                MetricFactory.newCounterWithSqlSessionContext(Metrics.HANG_SQL, v).once();
                SQLLogFilter.error("hang_sql=" + v.toString().replace('\n', ' '));
                // prevent log same sql twice
                v.queryId = -1;
            }
        });
        prevSqls = curSqls;

    }

    /**
     * kill all the sqlCtxs which are using the db with the qulifiedDbId when the cond is true
     *
     * @param qulifiedDbId
     * @param cond
     */
    public void killSqlCtxs(String qulifiedDbId, Predicate<SqlSessionContext> cond) {
        for (SqlSessionContext sqlCtx : getAllCtx()) {
            try {
                for (ServerSession ss : sqlCtx.shardedSession.values()) {
                    if (ss.getActiveQulifiedDbId().equals(qulifiedDbId) && cond.test(sqlCtx)) {
                        MetricFactory.newCounterWithSqlSessionContext(Metrics.SICK_KILL, sqlCtx)
                            .once();
                        SchedulerWorker.getInstance()
                            .enqueue(new SickManager(sqlCtx, ss.getActiveQulifiedDbId()));
                    }
                }
            } catch (Exception e) {
                logger.error(Objects.toString(e));
            }
        }
    }
}
