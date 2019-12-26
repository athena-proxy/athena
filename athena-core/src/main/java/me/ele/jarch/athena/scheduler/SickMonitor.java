package me.ele.jarch.athena.scheduler;

import me.ele.jarch.athena.constant.Metrics;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.server.pool.ServerSession;
import me.ele.jarch.athena.util.NoThrow;
import me.ele.jarch.athena.util.etrace.MetricFactory;
import me.ele.jarch.athena.worker.SchedulerWorker;
import me.ele.jarch.athena.worker.manager.SickManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Created by jinghao.wang on 16/1/29.
 */
public class SickMonitor {
    private static final Logger logger = LoggerFactory.getLogger(SickMonitor.class);

    protected final String groupName;
    protected volatile Set<String> manualSickedDBs = new HashSet<>();
    protected final Set<String> autoSickedDBs = new CopyOnWriteArraySet<>();

    public SickMonitor(String groupName) {
        this.groupName = groupName;
    }

    public boolean isSicked(String qulifiedDbId) {
        return manualSickedDBs.contains(qulifiedDbId) || autoSickedDBs.contains(qulifiedDbId);
    }

    public Set<String> getAllSickedDBs() {
        Set<String> allSickedDBs = new HashSet<>(manualSickedDBs);
        allSickedDBs.addAll(autoSickedDBs);
        return allSickedDBs;
    }

    public void sick(Set<String> qulifiedDbIds) {
        if (logger.isDebugEnabled()) {
            logger.debug(String
                .format("last manual sick values:[%s], newest manual sick values:[%s]",
                    manualSickedDBs, qulifiedDbIds));
        }
        manualSickedDBs = qulifiedDbIds;
        NoThrow.call(() -> {
            qulifiedDbIds.forEach(sickDB -> killSickedSqlCtx(sickDB));
        });
    }

    public void sick(String qulifiedDbId) {
        if (logger.isDebugEnabled()) {
            logger.debug(String
                .format("last auto sick values:[%s], new added auto sick value:[%s]", autoSickedDBs,
                    qulifiedDbId));
        }
        if (autoSickedDBs.contains(qulifiedDbId)) {
            return;
        }
        autoSickedDBs.add(qulifiedDbId);
        NoThrow.call(() -> {
            killSickedSqlCtx(qulifiedDbId);
        });
    }

    public boolean tryRecoverAutoSick(String recoverDB) {
        return autoSickedDBs.remove(recoverDB);
    }

    private void killSickedSqlCtx(String qulifiedDbId) {
        for (SqlSessionContext sqlCtx : HangSessionMonitor.getAllCtx()) {
            try {
                for (ServerSession ss : sqlCtx.shardedSession.values()) {
                    if (qulifiedDbId.equals(ss.getActiveQulifiedDbId())) {
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
