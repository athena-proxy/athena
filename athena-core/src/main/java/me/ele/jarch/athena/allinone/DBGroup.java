package me.ele.jarch.athena.allinone;

import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.constant.Metrics;
import me.ele.jarch.athena.constant.TraceNames;
import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.scheduler.Scheduler;
import me.ele.jarch.athena.scheduler.SchedulerGroup;
import me.ele.jarch.athena.server.pool.PoolId;
import me.ele.jarch.athena.server.pool.ResponseHandler;
import me.ele.jarch.athena.server.pool.ServerSessionPool;
import me.ele.jarch.athena.server.pool.ServerSessionPoolCenter;
import me.ele.jarch.athena.sql.seqs.SeqsCache;
import me.ele.jarch.athena.util.ZKCache;
import me.ele.jarch.athena.util.deploy.DALGroup;
import me.ele.jarch.athena.util.etrace.MetricFactory;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class DBGroup {
    private static final Logger logger = LoggerFactory.getLogger(DBGroup.class);

    private static final String OLD_MASTER = "OldMaster";
    private static final String NEW_MASTER = "NewMaster";
    public static final String DEFAULT_MASTER_NAME = "None";


    private final String groupName;
    private volatile List<String> masters = Collections.emptyList();
    private volatile String master = DEFAULT_MASTER_NAME;
    private volatile String sickId = "";
    private volatile String batchId = "";
    //@formatter:off
    /**
     *                 |-->Autocommit    --> ServerSessionPool
     * qualifiedDbId ---
     *                 |-->Non Autocommit --> ServerSessionPool
     */
    //@formatter:on
    Map<String, Map<AutocommitType, ServerSessionPool>> sessionPools = new TreeMap<>();
    private List<String> slaves = new ArrayList<>();
    private SchedulerGroup schedulerGroup;
    private DbGroupUpdater dbGroupUpdater;
    private final ZKCache zkCache;
    private volatile DALGroup dalGroup;

    private volatile DBConnectionInfo prevMasterDBInfo = null;

    private final SelectStrategy<String> masterSelector = new DefaultDBSelectStrategy();
    private final SelectStrategy<String> slaveSelector;

    public DBGroup(String group, ZKCache zkCache, SchedulerGroup schedulerGroup,
        DALGroup dalGroup) {
        this.groupName = group;
        this.schedulerGroup = schedulerGroup;
        this.zkCache = zkCache;
        this.dalGroup = dalGroup;
        this.slaveSelector =
            Objects.equals(Constants.PRIMARY_BACKUP, dalGroup.getSlaveSelectStrategy()) ?
                new PrimaryBackupDBSelectStrategy() :
                new DefaultDBSelectStrategy();
        attachBatchPool();
    }

    public void init() {
        if (Objects.nonNull(dbGroupUpdater)) {
            return;
        }
        this.dbGroupUpdater = new DbGroupUpdater(this, zkCache);
        dbGroupUpdater.startHeartBeatsJob();
    }

    /**
     * 为每个DBGroup附加虚拟的ServerSessionPool,
     */
    public void tryAttachFakePool(DBVendor dbVendor) {
        if (StringUtils.isNotEmpty(this.sickId)) {
            return;
        }
        Map<String, Map<AutocommitType, ServerSessionPool>> newPools = new TreeMap<>(sessionPools);
        ServerSessionPool fake4SickPool =
            ServerSessionPoolCenter.getInst().getSickServerSessionPool(dbVendor);
        String sickId = fake4SickPool.getDbConnInfo().getQualifiedDbId();
        newPools.put(sickId, Collections.singletonMap(AutocommitType.AUTOCOMMIT, fake4SickPool));
        this.sickId = sickId;
        this.sessionPools = newPools;
    }

    private void attachBatchPool() {
        Map<String, Map<AutocommitType, ServerSessionPool>> newPools = new TreeMap<>(sessionPools);
        ServerSessionPool batchPool =
            ServerSessionPoolCenter.getInst().getBatchOperateServerSessionPool();
        String batchId = batchPool.getDbConnInfo().getQualifiedDbId();
        newPools.put(batchId, Collections.singletonMap(AutocommitType.AUTOCOMMIT, batchPool));
        this.batchId = batchId;
        this.sessionPools = newPools;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setDalGroup(DALGroup dalGroup) {
        this.dalGroup = dalGroup;
        this.dbGroupUpdater.setDalGroup(dalGroup);
    }

    public DALGroup getDalGroup() {
        return dalGroup;
    }


    synchronized protected void addDbs(List<DBConnectionInfo> infos) {
        Map<String, Map<AutocommitType, ServerSessionPool>> newPools = new TreeMap<>(sessionPools);

        for (DBConnectionInfo info : infos) {
            String dbID = info.getQualifiedDbId();

            Map<AutocommitType, ServerSessionPool> autocommitTypedPool = new HashMap<>();
            if (info.getRole() != DBRole.SLAVE) {
                autocommitTypedPool.put(AutocommitType.AUTOCOMMIT,
                    ServerSessionPoolCenter.getInst().getServerSessionPool(info, true));
                autocommitTypedPool.put(AutocommitType.NON_AUTOCOMMIT,
                    ServerSessionPoolCenter.getInst().getServerSessionPool(info, false));
                tryClearSeqsCache(info);
            } else {
                autocommitTypedPool.put(AutocommitType.AUTOCOMMIT,
                    ServerSessionPoolCenter.getInst().getServerSessionPool(info, true));
            }
            Map<AutocommitType, ServerSessionPool> oldPools =
                newPools.put(dbID, autocommitTypedPool);
            if (Objects.nonNull(oldPools)) {
                oldPools.forEach((autocommitType, pool) -> {
                    logger.error("Cover old DB " + dbID);
                    pool.release();
                });
            }
        }
        this.schedulerGroup.addSchedulers(infos);
        refreshDBs(newPools);
    }

    private void tryClearSeqsCache(DBConnectionInfo nonSlaveInfo) {
        if (nonSlaveInfo.getRole() != DBRole.MASTER) {
            return;
        }
        if (Objects.isNull(prevMasterDBInfo)) {
            prevMasterDBInfo = nonSlaveInfo;
            return;
        }
        PoolId prevMasterId = new PoolId(prevMasterDBInfo, false);
        PoolId nextMasterId = new PoolId(nonSlaveInfo, false);
        if (!nextMasterId.equals(prevMasterId)) {
            SeqsCache.closeConn(prevMasterDBInfo, false);
        }
        prevMasterDBInfo = nonSlaveInfo;
    }

    synchronized public void removeDbs(List<DBConnectionInfo> infos) {
        Map<String, Map<AutocommitType, ServerSessionPool>> newPools = new TreeMap<>(sessionPools);
        DBConnectionInfo removedMaster = null;
        for (DBConnectionInfo info : infos) {
            Map<AutocommitType, ServerSessionPool> oldPools =
                newPools.remove(info.getQualifiedDbId());
            if (info.getRole() == DBRole.MASTER) {
                removedMaster = info;
            }
            if (oldPools == null) {
                logger.error("Try to remove a nonexistent DB : " + info.getQualifiedDbId());
                continue;
            }
            oldPools.forEach((autocommitType, oldPool) -> oldPool.release());
        }

        refreshDBs(newPools);
        this.schedulerGroup.removeSchedulers(infos);
        if (removedMaster != null) {
            SeqsCache.closeConn(removedMaster, true);
        }
    }

    public String getDBServer(DBRole role) throws QuitException {
        if (!isInitialized()) {
            logger.error("DBGroup:" + this.groupName + " has not been initialized");
            throw new QuitException("DBGroup:" + this.groupName + " has not been initialized");
        }

        if (dbGroupUpdater.isMultiMaster() && !this.masters.isEmpty()) {
            return masterSelector.select(masters);
        }

        if (role == DBRole.MASTER) {
            if (masterElected()) {
                return master;
            } else {
                throw new QuitException("DBGroup:" + this.groupName + " no available master pool");
            }
        } else {
            List<String> slavelist = this.slaves;
            if (slavelist.size() > 0) {
                return slaveSelector.select(slaves);
            } else if (masterElected()) {
                return master;
            } else {
                throw new QuitException("DBGroup:" + this.groupName + " no available slave pool");
            }
        }
    }

    public void acquireServerSession(final DBConnectionInfo targetDb,
        final SqlSessionContext sqlCtx, final ResponseHandler handler) throws QuitException {
        if (!dbGroupUpdater.isInitialized()) {
            logger.error("DBGroup:" + this.groupName + " has not been initialized");
            throw new QuitException("DBGroup:" + this.groupName + " has not been initialized");
        }
        String targetId = targetDb.getQualifiedDbId();
        DBRole targetRole = targetDb.getRole();
        Map<String, Map<AutocommitType, ServerSessionPool>> pools = this.sessionPools;
        Map<AutocommitType, ServerSessionPool> pool = pools.get(targetId);
        if (pool == null) {
            throw new QuitException(
                "DBGroup:" + this.groupName + " no available pool for the id:" + targetId);
        }
        if (sqlCtx.getHolder().getSickMonitor().isSicked(targetId)) {
            pools.get(sickId).get(AutocommitType.AUTOCOMMIT)
                .acquireServerSession(targetDb, handler, sqlCtx);
            return;
        }
        // 需要批量操作时，从批量操作session池中获取session
        if (sqlCtx.batchCond.isNeedBatchAnalyze()) {
            pools.get(batchId).get(AutocommitType.AUTOCOMMIT)
                .acquireServerSession(targetDb, handler, sqlCtx);
            return;
        }
        if (DBRole.SLAVE == targetRole || useAutoCommitMasterPool(sqlCtx, targetRole)) {
            pool.get(AutocommitType.AUTOCOMMIT).acquireServerSession(targetDb, handler, sqlCtx);
        } else {
            pool.get(AutocommitType.NON_AUTOCOMMIT).acquireServerSession(targetDb, handler, sqlCtx);
        }
    }

    /**
     * 使用autocommit的主库连接的情况如下 1. 需要master,但是当前SqlCtx不在事务状态 2. 需要master,且当前SqlCtx在mapping sharding状态
     *
     * @param sqlCtx
     * @param role
     * @return
     */
    private boolean useAutoCommitMasterPool(SqlSessionContext sqlCtx, DBRole role) {
        if (role == DBRole.SLAVE) {
            return false;
        }
        if (!sqlCtx.isInTransStatus()) {
            return true;
        }
        if (sqlCtx.isInMappingShardedMode) {
            return true;
        }
        return false;
    }

    public String getMasterName() {
        return master;
    }

    private void refreshDBs(Map<String, Map<AutocommitType, ServerSessionPool>> newPools) {
        String newmaster = this.master;
        List<String> newslaves = new ArrayList<>();
        List<String> newMasters = new ArrayList<>();
        boolean masterSelected = false;
        for (Map.Entry<String, Map<AutocommitType, ServerSessionPool>> entry : newPools
            .entrySet()) {
            DBConnectionInfo info = this.dbGroupUpdater.getCurrentActiveInfos().get(entry.getKey());
            if (info == null) {
                continue;
            }

            if (info.getRole() == DBRole.MASTER) {
                newmaster = chooseMaster(info, newmaster);
                masterSelected = true;
                //对tidb类的多个active master场景，选举出多个master
                if (dbGroupUpdater.isMultiMaster() && !DEFAULT_MASTER_NAME.equals(newmaster)) {
                    newMasters.add(entry.getKey());
                }
            } else if (info.getRole() == DBRole.SLAVE) {
                newslaves.add(entry.getKey());
            }
        }

        this.sessionPools = newPools;

        if (masterSelected) {
            if (!Objects.equals(this.master, newmaster) && !dbGroupUpdater.isMultiMaster()) {
                logger.error(String
                    .format("DBGroup(%s) has switched master from %s to %s", groupName, this.master,
                        newmaster));
                // 如果发生了master切换，发master切换告警，需要识别切换动作，第一次初始化选择master不告警
                if (dbGroupUpdater.isInitialized() && HeartBeatCenter.hasStartUp()) {
                    MetricFactory.newCounter(Metrics.HB_MASTER_SWITCHED)
                        .addTag(TraceNames.DBGROUP, this.groupName)
                        .addTag(TraceNames.DALGROUP, zkCache.getGroupName())
                        .addTag(OLD_MASTER, this.master).addTag(NEW_MASTER, newmaster).once();
                }
            }
            this.master = newmaster;
        } else {
            this.master = DEFAULT_MASTER_NAME;
            logger.error(String.format("DBGroup(%s) has lost master!", groupName));
        }
        this.slaves = newslaves;
        this.masters = newMasters;
    }

    private String chooseMaster(DBConnectionInfo info, String prevMaster) {
        DBConnectionInfo preMasterInfo =
            this.dbGroupUpdater.getCurrentActiveInfos().get(prevMaster);
        if (preMasterInfo != null && preMasterInfo.isAlive() && !preMasterInfo.isReadOnly()) {
            return prevMaster;
        }

        if (!info.isAlive() || info.isReadOnly()) {
            return DEFAULT_MASTER_NAME;
        }

        return info.getQualifiedDbId();
    }

    public boolean isInitialized() {
        if (Objects.isNull(this.dbGroupUpdater)) {
            return false;
        }
        return dbGroupUpdater.isInitialized();
    }

    enum AutocommitType {
        AUTOCOMMIT, NON_AUTOCOMMIT
    }

    public DbGroupUpdater getDbGroupUpdater() {
        return dbGroupUpdater;
    }

    public boolean masterElected() {
        return !DEFAULT_MASTER_NAME.equals(master) || (this.dbGroupUpdater.isMultiMaster()
            && !this.masters.isEmpty());
    }

    public List<String> getSlaves() {
        return slaves;
    }

    public synchronized void tryWarmUpServerSession(int expectedActiveSessions) {
        sessionPools.entrySet().stream()
            .filter(e -> !e.getKey().equals(sickId) && !e.getKey().equals(batchId)).forEach(
            entry -> entry.getValue().values()
                .forEach(serverSessionPool -> serverSessionPool.tryWarmUp(expectedActiveSessions)));
    }

    private class DefaultDBSelectStrategy implements SelectStrategy<String> {
        private final AtomicInteger index = new AtomicInteger(0);

        @Override public String select(List<String> candidates) throws QuitException {
            int minQueueSize = Integer.MAX_VALUE;
            int startIndex = index.getAndIncrement() % candidates.size();
            startIndex = startIndex < 0 ? (startIndex + candidates.size()) : startIndex;
            String target = "";
            for (int i = 0; i < candidates.size(); i++) {
                int current = (startIndex + i) % candidates.size();
                int queueSize = schedulerGroup.getScheduler(candidates.get(current))
                    .getQueueBlockingTaskCount();
                if (minQueueSize > queueSize) {
                    target = candidates.get(current);
                    minQueueSize = queueSize;
                }
            }
            return target;
        }
    }


    private class PrimaryBackupDBSelectStrategy implements SelectStrategy<String> {
        /**
         * 上次选出的普通库
         */
        private final AtomicReference<String> lastSelected = new AtomicReference<>(null);

        @Override public String select(List<String> candidates) throws QuitException {
            String primarySlave = "";
            final AtomicBoolean lastSelectedValid = new AtomicBoolean(false);
            for (String qulifiedDBId : candidates) {
                Scheduler scheduler = schedulerGroup.getScheduler(qulifiedDBId);
                if (Objects.isNull(scheduler)) {
                    continue;
                }
                String exRole =
                    scheduler.getInfo().getAttributeMap().getOrDefault(Constants.EX_ROLE, "");
                if (Constants.PRIMARY.equals(exRole)) {
                    // 记录primary slave
                    primarySlave = qulifiedDBId;
                }
                if (Objects.equals(qulifiedDBId, lastSelected.get())) {
                    lastSelectedValid.set(true);
                }
            }
            if (!primarySlave.isEmpty()) {
                return primarySlave;
            }
            String backupSlave = lastSelected.updateAndGet(
                prev -> lastSelectedValid.get() ? prev : candidates.iterator().next());
            MetricFactory.newCounter(Metrics.BACKUP_SLAVE_ONLINE)
                .addTag(TraceNames.DALGROUP, zkCache.getGroupName())
                .addTag(TraceNames.DBID, backupSlave);
            return backupSlave;
        }
    }
}
