package me.ele.jarch.athena.allinone;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import me.ele.jarch.athena.constant.Metrics;
import me.ele.jarch.athena.constant.TraceNames;
import me.ele.jarch.athena.netty.AthenaServer;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.scheduler.HangSessionMonitor;
import me.ele.jarch.athena.server.async.MasterHeartBeat;
import me.ele.jarch.athena.server.async.MasterHeartBeat.STATUS;
import me.ele.jarch.athena.util.*;
import me.ele.jarch.athena.util.deploy.DALGroup;
import me.ele.jarch.athena.util.etrace.DalGroupOperationType;
import me.ele.jarch.athena.util.etrace.MetricFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.*;
import static me.ele.jarch.athena.allinone.DBGroup.DEFAULT_MASTER_NAME;

public class DbGroupUpdater {
    private static final Logger logger = LoggerFactory.getLogger(DbGroupUpdater.class);

    private static final JobScheduler slaveHeartBeatScheduler =
        new JobScheduler("slaveHeartBeatScheduler");

    static {
        slaveHeartBeatScheduler.start();
    }

    private volatile Map<String, DBConnectionInfo> previousAllInfos = new TreeMap<>();
    private volatile Map<String, DBConnectionInfo> allInfos = new TreeMap<>();

    private volatile Map<String, DBConnectionInfo> previousActiveInfos = new TreeMap<>();
    private volatile Map<String, DBConnectionInfo> currentActiveInfos = new TreeMap<>();

    private final DBGroup dbGroup;
    private final ZKCache zkCache;

    private volatile boolean initialized = false;
    private final String groupName;
    private volatile DALGroup dalGroup;

    /**
     * id排序比较
     */
    private static final Comparator<DBConnectionInfo> DB_CONN_ID_CMP =
        Comparator.comparing(DbGroupUpdater::buildDbId);
    private static int MASTER_HEART_BEAT_WINDOW_MIILI = 10 * 1000;
    private FixedWindowJob fixedWindowJob =
        new FixedWindowJob(AthenaServer.vitalJobScheduler, MASTER_HEART_BEAT_WINDOW_MIILI);
    // 存放DBConnectionInfo和MasterHeartBeat组合的entity和DBConnectionInfo生成的ID的MAP
    private ConcurrentHashMap<String, DBHeartBeatEntity> masters = new ConcurrentHashMap<>();

    public DbGroupUpdater(DBGroup dbGroup, ZKCache zkCache) {
        this.dbGroup = dbGroup;
        this.zkCache = zkCache;
        this.groupName = dbGroup.getGroupName();
        this.dalGroup = dbGroup.getDalGroup();
    }

    public void setDalGroup(DALGroup dalGroup) {
        this.dalGroup = dalGroup;
    }

    /**
     * 用于收心跳的结果
     */
    public void startHeartBeatsJob() {
        HeartBeatUpdater heartBeatUpdator = new HeartBeatUpdater(this);
        heartBeatUpdator.start();
    }

    /**
     * 根据 dbcfg 创建心跳
     */
    public synchronized void updateByDBCfgs(List<DBConnectionInfo> newList) {
        // 只查询本次传入的dbInfo状态是否改变，以防止非master DB的状态被心跳同步
        List<DBConnectionInfo> changedDBInfos = sycnDbStatFromOldDBinfos(newList);

        updateDbInfos(newList, false);

        updateToDBGroup(changedDBInfos);

        updateHeartBeats();

    }

    public synchronized void updateByHeartBeat(List<DBConnectionInfo> newList) {
        // 只查询本次传入的dbInfo状态是否改变，以防止非master DB的状态被心跳同步
        List<DBConnectionInfo> changedDBInfos = getAndSyncChangedDBInfos(newList);

        updateDbInfos(newList, true);

        updateToDBGroup(changedDBInfos);
    }

    private void updateHeartBeats() {
        MapDifference<String, DBConnectionInfo> difference =
            Maps.difference(previousAllInfos, allInfos);
        difference.entriesOnlyOnLeft().forEach((id, info) -> {
            HeartBeatCenter.getInstance().deleteHeartBeat(info, dbGroup);
            masters.remove(id);
        });
        difference.entriesOnlyOnRight().forEach((id, info) -> {
            MasterHeartBeat mhb = HeartBeatCenter.getInstance().createHeartBeat(info, dbGroup);
            masters.put(id, new DBHeartBeatEntity(info, mhb));
        });
        difference.entriesDiffering().forEach((key, differentValue) -> {
            DBConnectionInfo oldDBInfo = differentValue.leftValue();
            HeartBeatCenter.getInstance().deleteHeartBeat(oldDBInfo, dbGroup);
            DBConnectionInfo newDBInfo = differentValue.rightValue();
            MasterHeartBeat mhb = HeartBeatCenter.getInstance().createHeartBeat(newDBInfo, dbGroup);
            masters.put(key, new DBHeartBeatEntity(newDBInfo, mhb));
        });
    }

    /**
     * 更新DBGroup里DB的状态，连接池等信息，对状态变化了的DB执行先移除在添加的逻辑进行更新
     *
     * @param changedDBInfos
     */
    private void updateToDBGroup(List<DBConnectionInfo> changedDBInfos) {

        List<DBConnectionInfo> removedList = new ArrayList<>();
        previousActiveInfos.forEach((id, info) -> {
            DBConnectionInfo newInfo = currentActiveInfos.get(id);
            if (newInfo == null || !newInfo.equals(info)) {
                removedList.add(info);
            }
        });
        List<DBConnectionInfo> addedList = new ArrayList<>();
        currentActiveInfos.forEach((id, info) -> {
            DBConnectionInfo oldInfo = previousActiveInfos.get(id);
            if (oldInfo == null || !oldInfo.equals(info)) {
                addedList.add(info);

            }
        });

        changedDBInfos.forEach(db -> {
            if (db.isActive()) {
                if (!addedList.contains(db)) {
                    addedList.add(db);
                }
            } else {
                if (!removedList.contains(db)) {
                    removedList.add(db);
                }
            }
        });

        if (!removedList.isEmpty()) {
            dbGroup.removeDbs(removedList);
        }

        if (!addedList.isEmpty()) {
            dbGroup.tryAttachFakePool(addedList.get(0).getDBVendor());
            dbGroup.addDbs(addedList);
        }

        if (!dbGroup.masterElected()) {
            logger.warn("There is no master configured for the group : " + this.groupName);
        }

        if (!this.isMultiMaster() && dbGroup.getSlaves().isEmpty()) {
            logger.warn("There is no slave configured for the group : " + this.groupName);
        }

        if (!initialized && !currentActiveInfos.isEmpty()) {
            initialized = true;
        }
    }

    private void updateDbInfos(List<DBConnectionInfo> newList, boolean isUpdateFromHeartBeat) {
        previousAllInfos = allInfos;
        Map<String, DBConnectionInfo> newDbInfos = new HashMap<>();
        // 如果是从heartbeat更新，则newList里只包含本组部分info信息，需要把原allInfos内容加入
        if (isUpdateFromHeartBeat) {
            for (Map.Entry<String, DBConnectionInfo> entry : allInfos.entrySet()) {
                newDbInfos.put(entry.getKey(), entry.getValue());
            }
        }
        newList.stream().filter(info -> info.getGroup().equals(this.groupName))
            .forEach(info -> newDbInfos.put(info.getQualifiedDbId(), info));
        if (!isUpdateFromHeartBeat) {
            sendDalGroupChangeInfo2Etrace(allInfos, newDbInfos);
        }
        allInfos = newDbInfos;

        this.previousActiveInfos = currentActiveInfos;
        Map<String, DBConnectionInfo> newActiveInfos = new TreeMap<>();
        newDbInfos.values().forEach(info -> {
            if (isValidDBInGroup(info, this.groupName)) {
                newActiveInfos.put(info.getQualifiedDbId(), info);
            }
        });
        currentActiveInfos = newActiveInfos;
    }

    boolean isInitialized() {
        return initialized;
    }

    public void stopWindow() {
        fixedWindowJob.stop();
    }

    Map<String, DBConnectionInfo> getCurrentActiveInfos() {
        return currentActiveInfos;
    }

    /**
     * Return whether a DB is online at this DALGroup, which is set by ZK
     *
     * @param info DB QualifiedDbId
     * @return if A DB is online at this DALGroup
     */
    public boolean isOnline(DBConnectionInfo info) {
        return !this.zkCache.getOfflineDbs().contains(info.getQualifiedDbId());
    }

    public Map<String, DBConnectionInfo> getAllInfos() {
        return allInfos;
    }

    private void sendDalGroupChangeInfo2Etrace(Map<String, DBConnectionInfo> oldDBInfos,
        Map<String, DBConnectionInfo> newAllInfos) {
        if (!oldDBInfos.equals(newAllInfos)) {
            MetricFactory.newCounter(Metrics.DALGROUP_CHANGE)
                .addTag(TraceNames.DALGROUP, this.zkCache.getGroupName())
                .addTag(TraceNames.OPERATION, DalGroupOperationType.MODIFY_DB.toString()).once();
        }
    }

    private List<DBConnectionInfo> getAndSyncChangedDBInfos(List<DBConnectionInfo> dbInfos) {
        return dbInfos.stream().filter(dbInfo -> dbInfo.getGroup().equals(groupName))
            .filter(dbInfo -> isStatusChanged(getDBInfoHbResult(dbInfo), dbInfo)).peek(dbInfo -> {
                dbInfo.resetStatusByHeartBeatResult(getDBInfoHbResult(dbInfo));
                tryKillSqlCtxsIfMasterOff(dbInfo);
            }).collect(toList());
    }

    private List<DBConnectionInfo> sycnDbStatFromOldDBinfos(List<DBConnectionInfo> newDbInfos) {
        List<DBConnectionInfo> changedDbInfos = new ArrayList<>();
        for (DBConnectionInfo dbInfo : newDbInfos) {
            if (currentActiveInfos.containsKey(dbInfo.getQualifiedDbId())) {
                DBConnectionInfo oldDbInfo = currentActiveInfos.get(dbInfo.getQualifiedDbId());
                if (isSameDb(dbInfo, oldDbInfo)) {
                    dbInfo.setRole(oldDbInfo.getRole());
                    dbInfo.setAlive(oldDbInfo.isAlive());
                    dbInfo.setReadOnly(oldDbInfo.isReadOnly());
                    changedDbInfos.add(dbInfo);
                }
            }
        }
        return changedDbInfos;
    }

    private boolean isSameDb(DBConnectionInfo oldDb, DBConnectionInfo newDb) {
        if (oldDb == newDb)
            return true;
        if (Objects.isNull(oldDb) || Objects.isNull(newDb))
            return false;
        if (!oldDb.getDatabase().equals(newDb.getDatabase()))
            return false;
        if (!oldDb.getGroup().equals(newDb.getGroup()))
            return false;
        if (!oldDb.getHost().equals(newDb.getHost()))
            return false;
        if (!oldDb.getId().equals(newDb.getId()))
            return false;
        if (oldDb.getPort() != newDb.getPort())
            return false;
        if (!oldDb.getPword().equals(newDb.getPword()))
            return false;
        if (oldDb.getDefaultRole() != newDb.getDefaultRole())
            return false;
        if (!oldDb.getUser().equals(newDb.getUser()))
            return false;
        return true;
    }

    private void tryKillSqlCtxsIfMasterOff(DBConnectionInfo dbInfo) {
        if (dbInfo == null || dbInfo.getRole() != DBRole.MASTER) {
            return;
        }
        if (dbInfo.isAlive() && !dbInfo.isReadOnly()) {
            return;
        }
        HangSessionMonitor.getInst()
            .killSqlCtxs(dbInfo.getQualifiedDbId(), SqlSessionContext::isInTransStatus);
    }

    private HeartBeatResult getDBInfoHbResult(DBConnectionInfo dbInfo) {
        return HeartBeatCenter.getInstance().getDBStatus(dbInfo);
    }

    private static boolean isStatusChanged(HeartBeatResult heartbeat, DBConnectionInfo info) {
        return !Objects.isNull(heartbeat) && (!Objects.equals(heartbeat.isAlive(), info.isAlive())
            || !Objects.equals(heartbeat.isReadOnly(), info.isReadOnly()) || !Objects
            .equals(info.getDefaultRole(), info.getRole()));
    }

    private static boolean isSlaveStatusChanged(DBHeartBeatEntity hbEntity) {
        HeartBeatResult heartBeatResult = hbEntity.masterHeartBeat.getResult();
        return !Objects.equals(heartBeatResult.isAlive(), hbEntity.dbInfo.isAlive()) || !Objects
            .equals(heartBeatResult.isReadOnly(), hbEntity.dbInfo.isReadOnly());
    }

    private boolean isValidDBInGroup(DBConnectionInfo info, String groupName) {
        if (!(groupName.equals(info.getGroup()))) {
            // 非本DBGroup
            return false;
        }
        if (!info.isAlive()) {
            // DB已挂
            return false;
        }
        if (!isOnline(info)) {
            // DB被临时下线(指slave)
            return false;
        }
        // DB正常且没有被人为下线
        if (Objects.equals(DBRole.MASTER, info.getRole())) {
            // master且readOnly=false
            return !info.isReadOnly();
        }
        // 纯slave开关关闭时, 永远返回true
        // 纯slave开关打开时, 返回readOnly的值
        return !isPureSlaveOnly() || info.isReadOnly();
    }

    private boolean isPureSlaveOnly() {
        return zkCache.isPureSlaveOnly() || dalGroup.isPureSlaveOnly();
    }

    public boolean isMultiMaster() {
        return this.allInfos.values().stream().anyMatch(dbInfo -> !dbInfo.getDistMatser());
    }

    public boolean isMasterConfigured() {
        return this.allInfos.values().stream().anyMatch(e -> e.getDefaultRole() == DBRole.MASTER);
    }

    private boolean isInWeakMasterMode() {
        return WeakMasterFilter.isWeakMasterDBGroup(dbGroup.getGroupName());
    }

    private static String buildDbId(DBConnectionInfo info) {
        return info.getGroup() + info.getDefaultRole() + info.getId() + info.getDatabase() + info
            .getHost() + info.getPort() + info.getUser() + info.getPword().hashCode();
    }

    private class HeartBeatUpdater {

        private final DbGroupUpdater dbGroupUpdater;

        HeartBeatUpdater(DbGroupUpdater dbGroupUpdater) {
            this.dbGroupUpdater = dbGroupUpdater;

        }

        private void start() {
            slaveHeartBeatScheduler.addJob("slave_heartbeat_job", 1000, this::checkSlaveHeartBeats);
            fixedWindowJob.start(this::checkMasterHeartBeats);
        }

        /**
         * check slave
         */
        private void checkSlaveHeartBeats() {
            for (DBHeartBeatEntity hbEntity : masters.values()) {
                if (hbEntity.dbInfo.getRole() != DBRole.MASTER && isSlaveStatusChanged(hbEntity)) {
                    dbGroupUpdater.updateByHeartBeat(Collections.singletonList(hbEntity.dbInfo));
                }
            }
        }

        /**
         * check master
         */
        private void checkMasterHeartBeats() {
            // Judge if current DBGroup opens fast fail
            if (FastFailFilter.checkDBGroupFastFailOpen(dbGroup.getGroupName())) {
                fixedWindowJob.setWindowSize(1000);
            } else {
                fixedWindowJob.setWindowSize(10 * 1000);
            }

            synchronized (dbGroupUpdater) {
                if (!isMasterConfigured()) {
                    removeFailedMasters(dbGroupUpdater.masters.values());
                    return;
                }
                Map<STATUS, List<DBHeartBeatEntity>> hbResult = takeHeartBeatResult();
                List<DBHeartBeatEntity> failedMasters =
                    hbResult.getOrDefault(STATUS.FAILED, Collections.emptyList());
                List<DBHeartBeatEntity> succeededMasters =
                    hbResult.getOrDefault(STATUS.SUCCESS, Collections.emptyList());
                List<DBHeartBeatEntity> burringMasters =
                    hbResult.getOrDefault(STATUS.BURR, Collections.emptyList());

                removeFailedMasters(failedMasters);
                // 取出心跳成功且readonly为false的DB用来选举出master
                List<DBConnectionInfo> qualifiedMasters = succeededMasters.stream()
                    .filter(hbEntity -> !hbEntity.masterHeartBeat.getResult().isReadOnly())
                    .map(hbEntity -> hbEntity.dbInfo).sorted(DB_CONN_ID_CMP).collect(toList());
                // 对支持多个master的场景，将所有active master都都选举成master
                if (this.dbGroupUpdater.isMultiMaster()) {
                    dbGroupUpdater.updateByHeartBeat(qualifiedMasters);
                } else if (isOKToElectMasters(qualifiedMasters)) {
                    logger.error(
                        "HeartBeatWindow: DBGroup(" + groupName + ") is about to elect master");
                    qualifiedMasters.get(0).setRole(DBRole.MASTER);
                    // 对符合条件的DB进行一次更新，触发master选举
                    dbGroupUpdater
                        .updateByHeartBeat(Collections.singletonList(qualifiedMasters.get(0)));
                }

                validateIfMasterAvaiable();

                if (!burringMasters.isEmpty()) {
                    logger.error(String.format(
                        "HeartBeatWindow: Master HeartBeat Burr: DBGroup(%s) current master is %s",
                        groupName, dbGroup.getMasterName()));
                }
                // 如果有多个可用的master，按规则选择master后，发多可用master告警
                raiseAlaram4MutiMaster();
            }
        }

        private Map<STATUS, List<DBHeartBeatEntity>> takeHeartBeatResult() {
            return masters.values().stream().collect(
                groupingBy(hbEntity -> hbEntity.masterHeartBeat.takeWindowResult().status()));
        }

        private String heartBeatStatusStr(Collection<DBHeartBeatEntity> hbEntities) {
            return hbEntities.stream().map(
                hbEntity -> buildDbId(hbEntity.dbInfo) + ":" + hbEntity.masterHeartBeat.getResult()
                    .toString()).collect(joining(","));
        }

        private boolean isOKToElectMasters(List<DBConnectionInfo> qualifiedMasters) {
            return !qualifiedMasters.isEmpty() && !dbGroup.masterElected();
        }

        private void removeFailedMasters(Collection<DBHeartBeatEntity> failedMasters) {
            failedMasters.stream().filter(hbEntity -> hbEntity.dbInfo.getRole() == DBRole.MASTER)
                .filter(
                    hbEntity -> currentActiveInfos.containsKey(hbEntity.dbInfo.getQualifiedDbId()))
                // backup的master的readonly=true,因此也会被作为failedMaster传进来。过滤掉这类DB
                .filter(hbEntity -> !isQualifiedBackupMaster(hbEntity)).forEach(hbEntity -> {
                hbEntity.dbInfo.setRole(hbEntity.dbInfo.getDefaultRole());
                dbGroupUpdater.updateByHeartBeat(Collections.singletonList(hbEntity.dbInfo));
            });
        }

        private boolean isQualifiedBackupMaster(DBHeartBeatEntity hbEntity) {
            return hbEntity.dbInfo.isAlive() && !hbEntity.dbInfo.getQualifiedDbId()
                .equals(dbGroup.getMasterName()) && hbEntity.masterHeartBeat.getResult()
                .isReadOnly();
        }

        private void validateIfMasterAvaiable() {
            if (!dbGroup.masterElected() && !isInWeakMasterMode() && isMasterConfigured()) {
                logger.error(String.format(
                    "HeartBeatWindow: DBGroup(%s) has lost master. HeartBeatStatus: %s. Current master is %s",
                    groupName, heartBeatStatusStr(masters.values()), DEFAULT_MASTER_NAME));
                // 如果没有可用的master，告警
                if (HeartBeatCenter.hasStartUp()) {
                    MetricFactory.newCounter(Metrics.HB_NO_AVAILABLE_MASTER)
                        .addTag(TraceNames.DALGROUP, zkCache.getGroupName())
                        .addTag(TraceNames.DBGROUP, groupName).once();
                }
            }
        }

        /**
         * 当dalgroup中有多个不同物理db满足slaveFlag=false或readonly=false时，则告警。
         */
        private void raiseAlaram4MutiMaster() {
            boolean hasMutiMaster =
                HeartBeatCenter.hasStartUp() && !this.dbGroupUpdater.isMultiMaster()
                    && hasMultiQualifiedMaster();
            if (hasMutiMaster) {
                logger.error(String.format("%s, group:%s", Metrics.HB_MULTI_AVAILABLE_MASTER,
                    dbGroup.getMasterName()));
                MetricFactory.newCounter(Metrics.HB_MULTI_AVAILABLE_MASTER)
                    .addTag(TraceNames.DBGROUP, dbGroup.getMasterName()).once();
            }
        }

        private boolean hasMultiQualifiedMaster() {
            List<DBConnectionInfo> qualifiedMasters =
                masters.values().stream().filter(this::isQualifiedMaster).map(hbe -> hbe.dbInfo)
                    .collect(Collectors.toList());
            if (qualifiedMasters.isEmpty() || qualifiedMasters.size() == 1) {
                return false;
            }
            return qualifiedMasters.stream().map(DBConnectionId::buildId).distinct().count() > 1;
        }

        private boolean isQualifiedMaster(DBHeartBeatEntity hbe) {
            HeartBeatResult result = hbe.masterHeartBeat.getResult();
            if (!result.isAlive()) {
                return false;
            }
            return !result.isReadOnly();
        }
    }


    private class DBHeartBeatEntity {
        private final DBConnectionInfo dbInfo;
        private final MasterHeartBeat masterHeartBeat;

        DBHeartBeatEntity(DBConnectionInfo dbInfo, MasterHeartBeat masterHeartBeat) {
            this.dbInfo = dbInfo;
            this.masterHeartBeat = masterHeartBeat;
        }
    }
}
