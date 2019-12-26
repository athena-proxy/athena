package me.ele.jarch.athena.server.async;

import me.ele.jarch.athena.allinone.DBConnectionInfo;
import me.ele.jarch.athena.allinone.DBRole;
import me.ele.jarch.athena.allinone.HeartBeatCenter;
import me.ele.jarch.athena.allinone.HeartBeatResult;
import me.ele.jarch.athena.constant.Metrics;
import me.ele.jarch.athena.netty.AthenaServer;
import me.ele.jarch.athena.sql.ResultSet;
import me.ele.jarch.athena.util.CyclicList;
import me.ele.jarch.athena.util.Job;
import me.ele.jarch.athena.util.TimeFormatUtil;
import me.ele.jarch.athena.util.etrace.ImpatientMetricsSender;
import me.ele.jarch.athena.util.etrace.MetricFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by zhengchao on 16/7/7.
 */
public class MasterHeartBeat implements HeartBeat {
    private static final Logger logger = LoggerFactory.getLogger(MasterHeartBeat.class);
    private static long DEFAULT_INTERVAL = 1000;
    private final DBConnectionInfo dbConnInfo;
    private final AtomicReference<Job> job = new AtomicReference<Job>();
    private AsyncHeartBeat connection;
    private final HeartBeatResult result;
    private CyclicList<STATUS> resultList = new CyclicList<>(10);
    private volatile boolean destroyed = false;
    private final AsyncResultSetHandler handler = (rs, isdone, reason) -> {
        try {
            // no packet
            if (!isdone) {
                heartbeatFailed(reason);
                return;
            }
            // get packet but err
            if (Objects.nonNull(rs.getErr())) {
                heartbeatFailed(rs.getErr().errorMessage);
                return;
            }
            // get packet and ok
            heartbeatSucess(rs);
        } catch (Exception t) {
            attachHeartBeatJob(DEFAULT_INTERVAL);
            logger
                .error("Exception AsyncResultSetHandler " + getDbConnInfo().getQualifiedDbId(), t);
        }
    };

    private void heartbeatFailed(String reason) {
        synchronized (this.result) {
            this.result.incrementAndGetFailedTimes();
            errLogCheck(reason + " result for db: " + this.dbConnInfo);
            resultList.add(STATUS.FAILED);
            if (this.result.getRole() == DBRole.MASTER) {
                result.setAlive(false);
                attachHeartBeatJob(DEFAULT_INTERVAL);
            } else {
                heartbeatFailedForSlave(reason);
            }
        }
    }

    private void heartbeatFailedForSlave(String reason) {
        int failedCount = this.result.getFailedTimes();
        if (failedCount > HeartBeatCenter.getMaxMissedHeartbeat() && this.result.isAlive()) {
            this.result.setAlive(false);
            logger.warn("heartbeat.servicedown " + this.dbConnInfo.getQualifiedDbId());
            MetricFactory.newCounterWithDBConnectionInfo(Metrics.HB_SRV_DOWN, dbConnInfo).once();
        }

        if (failedCount < 10) {
            attachHeartBeatJob(100);
        } else if (failedCount < 20) {
            attachHeartBeatJob(500);
        } else if (failedCount < 100) {
            attachHeartBeatJob(1000);
        } else {
            attachHeartBeatJob(3000);
        }
    }

    private void heartbeatSucess(ResultSet rs) {
        synchronized (this.result) {
            boolean readOnly = connection.isReadOnly(rs);
            boolean slaveFlag = connection.hasSlaveFlag();
            if (readOnly || slaveFlag) {
                resultList.add(STATUS.FAILED);
            } else {
                resultList.add(STATUS.SUCCESS);
            }
            this.result.setReadOnly(readOnly);
            result.setSlaveFlag(slaveFlag);
            result.setAlive(true);
            if (result.getFailedTimes() > 0) {
                String metricType = getMetricType(Metrics.HB_FAILED);
                ImpatientMetricsSender.senders.remove(metricType);
                if (result.getRole() != DBRole.MASTER) {
                    String slaveMetricType = getMetricType(Metrics.HB_SRV_DOWN);
                    ImpatientMetricsSender.senders.remove(slaveMetricType);
                }
            }
            result.setFailedTimes(0);
            if (this.result.getRole() != DBRole.MASTER) {
                heartbeatSucessAsSlave(rs);
            }
            attachHeartBeatJob(DEFAULT_INTERVAL);
        }
    }

    private void heartbeatSucessAsSlave(ResultSet rs) {
        if (!this.result.isAlive()) {
            MetricFactory.newCounterWithDBConnectionInfo(Metrics.HB_SRV_UP, dbConnInfo).once();
            logger.warn("DB heartbeat.serviceup " + dbConnInfo);
        }
    }

    public MasterHeartBeat(DBConnectionInfo dbConnInfo) {
        this.dbConnInfo = Objects.requireNonNull(dbConnInfo,
            () -> "MasterHeartBeat:DBConnectionInfo must not be null,dbConnInfo=" + dbConnInfo);
        this.connection = AsyncHeartBeat.newAsyncHeartBeat(dbConnInfo, handler);
        this.result = new HeartBeatResult(dbConnInfo.getGroup(), dbConnInfo.getId(), true, true, 0,
            dbConnInfo.getRole());
    }

    synchronized private void doHeartBeat() {
        try {
            if (destroyed) {
                this.destroy();
                return;
            }
            if (!connection.isActive() || !connection.isAvailable()) {
                connection.doQuit("Heartbeat client is not ready");
                connection = AsyncHeartBeat.newAsyncHeartBeat(dbConnInfo, handler);
            }
            if (connection.isOverdue()) {
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format(
                        "overdue, close heartbeat client: %s, connectionId: %s, with birthdayInMill: %s",
                        connection.getDbConnInfo().getQualifiedDbId(), connection.getConnectionId(),
                        TimeFormatUtil.formatTimeStamp(connection.getBirthday())));
                }
                closeAsyncHeartBeat();
                connection = AsyncHeartBeat.newAsyncHeartBeat(dbConnInfo, handler);
            }
            // 从这里开始心跳，INIT -> HANDSHAKE_AUTH -> ... -> QUERY
            if (!connection.doAsyncExecute()) {
                connection.doQuit("Failed to start heartbeat");
            }
        } catch (Exception e) {
            logger.error(Objects.toString(e));
        }
    }

    synchronized public void start() {
        if (job.get() != null) {
            return;
        }
        this.attachHeartBeatJob(DEFAULT_INTERVAL);
    }

    private void attachHeartBeatJob(long interval) {
        Job newjob = AthenaServer.vitalJobScheduler
            .addJob(dbConnInfo.getQualifiedDbId() + ".heartbeat", interval, () -> {
                this.doHeartBeat();
            }, true);

        Job currentJob = this.job.getAndSet(newjob);
        if (currentJob != null) {
            currentJob.cancel();
        }
    }

    synchronized public void destroy() {
        this.destroyed = true;
        closeAsyncHeartBeat();
    }

    private void closeAsyncHeartBeat() {
        Job currentJob = this.job.get();
        if (currentJob != null) {
            currentJob.cancel();
        }
        AsyncHeartBeat currentConn = this.connection;
        if (currentConn != null) {
            currentConn.close();
        }
    }


    protected void errLogCheck(String msg) {
        int failedTimes = result.getFailedTimes();
        if (failedTimes <= 3) {
            logger.error(msg);
        } else if (failedTimes % 21 == 0) {
            logger.error(msg);
        }
    }

    /**
     * the master has chance to be selected as active master if true
     *
     * @return
     */
    public boolean canBeActiveMaster() {
        if (!this.dbConnInfo.isShadowDb()) {
            return true;
        }
        return AthenaServer.globalZKCache.isAllowShadowdb();
    }

    public DBConnectionInfo getDbConnInfo() {
        return dbConnInfo;
    }

    public HeartBeatResult getResult() {
        return new HeartBeatResult(result.getGroup(), result.getId(), result.isAlive(),
            result.isReadOnly(), result.getFailedTimes(), result.getRole(),
            result.getQualifiedDbId(), result.isSlaveFlag());
    }

    public WindowResult takeWindowResult() {
        int succTimes = 0;
        int failedTimes = 0;
        List<STATUS> resList = resultList.takeAll();
        for (STATUS status : resList) {
            if (status == STATUS.SUCCESS) {
                succTimes++;
            } else if (status == STATUS.FAILED) {
                failedTimes++;
            }
        }

        WindowResult ret;
        if (succTimes == 0 && failedTimes != 0) {
            /*
             * Only when all heartbeats within this window failed, we regard this master mysql instance as failed.
             */
            ret = new WindowResult(STATUS.FAILED, this.result.isAlive(), this.dbConnInfo.isActive(),
                this.result.isReadOnly(), this.result.isSlaveFlag());
        } else if (succTimes != 0 && failedTimes == 0) {
            ret =
                new WindowResult(STATUS.SUCCESS, this.result.isAlive(), this.dbConnInfo.isActive(),
                    this.result.isReadOnly(), this.result.isSlaveFlag());
        } else if (succTimes != 0 && failedTimes != 0) {
            ret = new WindowResult(STATUS.BURR, this.result.isAlive(), this.dbConnInfo.isActive(),
                this.result.isReadOnly(), this.result.isSlaveFlag());
            logger.error(String
                .format("Master(%s) HeartBeat Burr: [%s]", dbConnInfo.getQualifiedDbId(),
                    heartBeatStatusStr(resList)));
        } else {
            /*
             * In this window, both succTimes and failedTimes are 0, we regard this scenario as FAILED. Wait for the next window.
             */
            ret = new WindowResult(STATUS.FAILED, this.result.isAlive(), this.dbConnInfo.isActive(),
                this.result.isReadOnly(), this.result.isSlaveFlag());
        }

        return ret;
    }

    private static String heartBeatStatusStr(List<STATUS> heartBeatStatus) {
        StringBuilder sb = new StringBuilder();
        if (heartBeatStatus.size() > 0) {
            sb.append(heartBeatStatus.get(0));
            for (int i = 1; i < heartBeatStatus.size(); ++i) {
                sb.append(",").append(heartBeatStatus.get(i));
            }
        }

        return sb.toString();
    }

    public enum STATUS {
        SUCCESS, FAILED, BURR
    }


    public class WindowResult {
        private STATUS status;
        private boolean isAlive;
        private boolean isActive;
        private boolean isReadOnly;
        private boolean slaveFlag;

        public WindowResult(STATUS status, boolean alive, boolean active, boolean readOnly,
            boolean slaveFlag) {
            this.status = status;
            this.isAlive = alive;
            this.isActive = active;
            this.isReadOnly = readOnly;
            this.slaveFlag = slaveFlag;
        }

        public STATUS status() {
            return this.status;
        }

        public boolean isAlive() {
            return this.isAlive;
        }

        public boolean isActive() {
            return this.isActive;
        }

        public boolean isReadOnly() {
            return this.isReadOnly;
        }

        public String toString() {
            return String
                .format("WindowResult[status=%s,alive=%s,active=%s,readonly=%s,slaveFlag=%s]",
                    status, isAlive, isActive, isReadOnly, slaveFlag);
        }
    }

    private String getMetricType(String traceName) {
        return traceName + dbConnInfo.getQualifiedDbId();
    }

}
