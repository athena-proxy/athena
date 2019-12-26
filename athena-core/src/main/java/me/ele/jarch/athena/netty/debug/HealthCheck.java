package me.ele.jarch.athena.netty.debug;

import me.ele.jarch.athena.allinone.DBConnectionInfo;
import me.ele.jarch.athena.allinone.DBRole;
import me.ele.jarch.athena.allinone.HeartBeatCenter;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.netty.AthenaFrontServer;
import me.ele.jarch.athena.netty.AthenaServer;
import me.ele.jarch.athena.scheduler.DBChannelDispatcher;
import me.ele.jarch.athena.scheduler.HangSessionMonitor;
import me.ele.jarch.athena.scheduler.MulScheduler;
import me.ele.jarch.athena.util.AthenaConfig;
import me.ele.jarch.athena.util.WeakMasterFilter;
import me.ele.jarch.athena.util.curator.ZkCurator;
import me.ele.jarch.athena.util.health.check.DalGroupHealthCheck;
import me.ele.jarch.athena.util.health.check.DalGroupHealthCheckStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;

public class HealthCheck {

    private static final Logger logger = LoggerFactory.getLogger(HealthCheck.class);
    private static final int MULTIPLE = 10;
    private static final int THRESHOLD_HEAP_RATIO = 80;
    private static final int THRESHOLD_NONHEAP_RATIO = 80;
    private static final int THRESHOLD_MULSCHEDULER_SLOT_RATIO = 80;
    private final int THRESHOLD_CLIENT_COUNT = AthenaConfig.getInstance().getThresholdClientCount();

    private StringBuilder checkMessage = new StringBuilder();

    private static final String CHECKED_HEAP_RATIO =
        "Checked heap ratio exceeds %d,maxHeap= %d,usedHeap= %d ";
    private static final String CHECKED_NONHEAP_RATIO =
        "Checked nonheap ratio exceeds %d,maxNonHeap= %d,usedNonHeap= %d ";
    private static final String CHECKED_MAX_QUEUE =
        "Checked queue exceeds max_active_db multi %d,Id= %s ,MaxDBSession= %d,QueueCount= %d ";
    private static final String CHECKED_DB = "Checked DB status unexpected ,Id= %s";
    private static final String CHECKED_CLIENT_COUNT =
        "Checked client count exceeds %d,clientCount=%d ";
    private static final String CHECKED_SICK = "Checked DB is sick, Id=%s";
    private static final String CHECKED_HAS_LISTEN_PORT =
        "Checked has not listening port,hasListenedPorts=%s";
    private static final String CHECK_ZK_CONNECTED = "Check ZooKeeper connected=%b";
    private static final String CHECK_START_PHRASE = "Check AthenaServer perfectStart=%s";
    private static final String CHECK_MULSCHEDULER_CAPACITY =
        "Check MulScheduler queue exceeds %d, maxSlots = %d, warnSlots = %d";
    private static final String CHECK_ROLE = "Check DAL role=%s";
    private static final String CHECK_DAL_GROUP =
        "Check DalGroup connectivity failed, dalGroup=%s, status=%s";

    public String getCheckMessage() {
        return checkMessage.toString() + "\n";
    }

    public boolean check() {
        boolean result = false;
        try {
            result = checkDB() & checkSemaphore() & checkMemory() & checkClientCount() & checkSick()
                & checkHasListenedPorts() & checkZK() & checkStart() & checkMulScheduler()
                & checkRole() & checkDalGroupHealth();  //NOSONAR
        } catch (Exception t) {
            result = false;
            logError("ExceptionInHealthCheck:" + t.getMessage(), "ExceptionInHealthCheck",
                t.getMessage(), "NoException");
            logger.error("While checking health", t);
        }
        return result;
    }

    private boolean checkMemory() {
        MemoryMXBean m = ManagementFactory.getMemoryMXBean();
        long maxHeap = m.getHeapMemoryUsage().getMax();
        long usedHeap = m.getHeapMemoryUsage().getUsed();

        long maxNonHeap = m.getHeapMemoryUsage().getMax();
        long usedNonHeap = m.getHeapMemoryUsage().getUsed();
        long currHeapRaio = usedHeap / maxHeap * 100;
        boolean rel = true;
        if (currHeapRaio > THRESHOLD_HEAP_RATIO) {
            String err = String.format(CHECKED_HEAP_RATIO, THRESHOLD_HEAP_RATIO, maxHeap, usedHeap);
            logError(err, "HeapRatio", currHeapRaio, THRESHOLD_HEAP_RATIO);
            rel = false;
        }

        long currNonHeapRaio = usedNonHeap / maxNonHeap * 100;
        if (currNonHeapRaio > THRESHOLD_NONHEAP_RATIO) {
            String err = String
                .format(CHECKED_NONHEAP_RATIO, THRESHOLD_NONHEAP_RATIO, maxNonHeap, usedNonHeap);
            logError(err, "NonHeapRatio", currNonHeapRaio, THRESHOLD_NONHEAP_RATIO);
            rel = false;
        }

        return rel;

    }

    private boolean checkSemaphore() {
        boolean notHealth[] = {false};
        DBChannelDispatcher.getHolders().values().stream().forEach(holder -> {
            holder.getScheds().stream().forEach(sched -> {
                int threshold = (int) Math.ceil(sched.getMaxActiveDBSessions()) * MULTIPLE;
                int curQueueSize = sched.getQueueBlockingTaskCount();
                if (curQueueSize > threshold) {
                    String err = String
                        .format(CHECKED_MAX_QUEUE, MULTIPLE, sched.getInfo().getQualifiedDbId(),
                            curQueueSize, sched.getQueueBlockingTaskCount());
                    logError(err, "maxQueue." + sched.getName(), curQueueSize, threshold);
                    notHealth[0] = true;
                }
            });

        });
        return !notHealth[0];
    }

    private boolean checkDB() {
        boolean notHealthy[] = {false};

        DBChannelDispatcher.getHolders().values().forEach(dispatcher -> {
            dispatcher.getDbGroups().values().stream()
                .flatMap(dbGroup -> dbGroup.getDbGroupUpdater().getAllInfos().values().stream())
                .filter(dbInfo -> !HeartBeatCenter.getInstance().getDBStatus(dbInfo).isAlive())
                .forEach(dbInfo -> {
                    notHealthy[0] = true;
                    logForUnhealthyDB(dbInfo, "dbAlive", false, true);
                });
        });

        // This handles when there is no valid master
        DBChannelDispatcher.getHolders().values().forEach(dispatcher -> {
            dispatcher.getDbGroups().values().stream().filter(dbGroup -> !dbGroup.masterElected())
                .filter(dbGroup -> dbGroup.getDbGroupUpdater().isMasterConfigured())
                .filter(dbGroup -> !WeakMasterFilter.isWeakMasterDBGroup(dbGroup.getGroupName()))
                .forEach(dbGroup -> {
                    dbGroup.getDbGroupUpdater().getAllInfos().values().stream().
                        filter(dbInfo -> dbInfo.getRole() == DBRole.MASTER).forEach(info -> {
                        notHealthy[0] = true;
                        logForUnhealthyDB(info, "MasterDBActive", false, true);
                    });
                });
        });

        return !notHealthy[0];
    }

    private void logForUnhealthyDB(DBConnectionInfo info, String label, Boolean actualState,
        Boolean expectedState) {
        String err = String.format(CHECKED_DB, info.getQualifiedDbId());
        logError(err, label + "." + info.getQualifiedDbId(), actualState, expectedState);
        logger.error(err);
    }

    private boolean isOnline(DBConnectionInfo dbInfo) {
        boolean isOnline = DBChannelDispatcher.getHolders().values().stream().anyMatch(dalGroup -> {
            return dalGroup.isInScope(dbInfo.getGroup()) && !dalGroup.getZKCache().getOfflineDbs()
                .contains(dbInfo.getQualifiedDbId());
        });
        return isOnline;
    }

    private boolean checkClientCount() {
        int size = HangSessionMonitor.getAllCtx().size();
        if (size > THRESHOLD_CLIENT_COUNT) {
            String err = String.format(CHECKED_CLIENT_COUNT, THRESHOLD_CLIENT_COUNT, size);
            logError(err, "clientCount", size, THRESHOLD_CLIENT_COUNT);
            return false;
        }
        return true;
    }

    /**
     * 检查dal是否有监听端口服务
     *
     * @return
     */
    private boolean checkHasListenedPorts() {
        boolean hasListenedPorts = AthenaFrontServer.getInstance().hasListenedPorts();
        if (!hasListenedPorts) {
            String err = String.format(CHECKED_HAS_LISTEN_PORT, false);
            logError(err, "hasListenedPorts", false, true);
            return false;
        }
        return true;
    }

    private boolean checkSick() {
        boolean notHealth[] = {false};
        DBChannelDispatcher.getHolders().values()
            .forEach(holder -> holder.getSickMonitor().getAllSickedDBs().forEach(sickedDB -> {
                String err = String.format(CHECKED_SICK, sickedDB);
                logError(err, "sick." + sickedDB, "on", "off");
                notHealth[0] = true;
            }));

        return !notHealth[0];
    }

    private boolean checkZK() {
        boolean connected = ZkCurator.isConnected();
        if (!connected) {
            String err = String.format(CHECK_ZK_CONNECTED, connected);
            logError(err, "zookeeperConnected", connected, true);
        }
        return connected;
    }

    private boolean checkStart() {
        boolean perfectStart = AthenaServer.isPerfectStart();
        if (!perfectStart) {
            String err = String.format(CHECK_START_PHRASE, perfectStart);
            logError(err, "AthenaServer start pefectly", AthenaServer.startError(),
                "started perfectly");
        }
        return perfectStart;
    }

    private boolean checkMulScheduler() {
        int usedSlots = MulScheduler.getInstance().size();
        int maxSlots = MulScheduler.getInstance().capacity();
        int thresholdSlots = (maxSlots * THRESHOLD_MULSCHEDULER_SLOT_RATIO) / 100;
        if (usedSlots > thresholdSlots) {
            String err =
                String.format(CHECK_MULSCHEDULER_CAPACITY, usedSlots, maxSlots, thresholdSlots);
            logError(err, "MulScheduler slots", usedSlots, thresholdSlots);
            return false;
        }
        return true;
    }

    private boolean checkRole() {
        boolean isExpectedRole = !"mediator".equals(Constants.ROLE);
        if (!isExpectedRole) {
            String err = String.format(CHECK_ROLE, Constants.ROLE);
            logError(err, "abnormal dal role", Constants.ROLE, "normal");
        }
        return isExpectedRole;
    }

    private boolean checkDalGroupHealth() {
        boolean success[] = {true};
        DalGroupHealthCheck.getInstance().getUnhealthyDalGroup().forEach((dalGroup, status) -> {
            String err = String.format(CHECK_DAL_GROUP, dalGroup, status.toString());
            logError(err, "dalGroupHealthCheck",
                "dalGroupHealthy." + dalGroup + "." + status.toString(),
                DalGroupHealthCheckStatus.HEART_BEAT_SUCCESS);
            success[0] = false;
        });
        DalGroupHealthCheck.getInstance().getShardTableStatus().forEach((dalGroup, status) -> {
            if (status != DalGroupHealthCheckStatus.HEART_BEAT_SUCCESS) {
                String err = String.format(CHECK_DAL_GROUP, dalGroup, status.toString());
                logError(err, "dalGroupHealthCheck",
                    "dalGroupHealthy." + dalGroup + "." + status.toString(),
                    DalGroupHealthCheckStatus.HEART_BEAT_SUCCESS);
                success[0] = false;
            }
        });
        return success[0];
    }


    private void logError(String err, String curr, Object currValue, Object critriaValue) {
        checkMessage.append(curr).append(": ").append(currValue).append("; Criteria:")
            .append(critriaValue).append("\n");
        logger.error(err);
    }

}
