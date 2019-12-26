package me.ele.jarch.athena.scheduler;

import com.github.mpjct.jmpjct.util.ErrorCode;
import me.ele.jarch.athena.allinone.DBConnectionInfo;
import me.ele.jarch.athena.allinone.DBGroup;
import me.ele.jarch.athena.allinone.DBRole;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.constant.EventTypes;
import me.ele.jarch.athena.constant.Metrics;
import me.ele.jarch.athena.constant.TraceNames;
import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.netty.SessionQuitTracer;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.sharding.ShardingRouter;
import me.ele.jarch.athena.sql.CmdQuery;
import me.ele.jarch.athena.sql.seqs.GlobalId;
import me.ele.jarch.athena.util.AthenaConfig;
import me.ele.jarch.athena.util.NoThrow;
import me.ele.jarch.athena.util.WeakMasterFilter;
import me.ele.jarch.athena.util.ZKCache;
import me.ele.jarch.athena.util.curator.ZkCuratorUtil;
import me.ele.jarch.athena.util.deploy.DALGroup;
import me.ele.jarch.athena.util.deploy.OrgConfig;
import me.ele.jarch.athena.util.deploy.dalgroupcfg.DalGroupConfig;
import me.ele.jarch.athena.util.etrace.DalGroupOperationType;
import me.ele.jarch.athena.util.etrace.MetricFactory;
import me.ele.jarch.athena.util.etrace.TraceEnhancer;
import me.ele.jarch.athena.worker.SchedulerWorker;
import me.ele.jarch.athena.worker.manager.ManualKillerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

public class DBChannelDispatcher {

    private static Logger logger = LoggerFactory.getLogger(DBChannelDispatcher.class);

    public final AtomicInteger cCnt = new AtomicInteger();
    public final AtomicInteger opencCnt = new AtomicInteger();

    //key: dalGroupName, value:DBChannelDispatcher
    private final static Map<String, DBChannelDispatcher> holders = new ConcurrentHashMap<>();

    /**
     * DalGroupConfig map used to generate the dalgroup-[].json file
     */
    public static final Map<String, DalGroupConfig> TEMP_DAL_GROUP_CONFIGS =
        new ConcurrentHashMap<>();
    /**
     * DAL_GROUP_CONFIGS holds dalGroup configs loaded from the JSon file
     */
    private static final Map<String, DalGroupConfig> DAL_GROUP_CONFIGS = new ConcurrentHashMap<>();

    private volatile DALGroup dalGroup;
    private volatile DalGroupConfig dalGroupConfig = DalGroupConfig.DUMMY;
    private Map<String, DBGroup> dbGroups = new ConcurrentHashMap<>();
    private SchedulerGroup schedulerGroup;
    private volatile Set<String> shardingDb = new HashSet<>();
    private volatile ShardingRouter shardingRouter = ShardingRouter.defaultShardingRouter;
    private AtomicBoolean dalConfInitialized = new AtomicBoolean(false);

    private final Map<String, SQLTrafficPolicing> sqlTrafficPolicings = new ConcurrentHashMap<>();

    public DBGroup getHomeDbGroup() {
        return getDbGroup(dalGroup.getDbGroup());
    }

    public DALGroup getDalGroup() {
        return dalGroup;
    }

    public DBGroup getDbGroup(String dbGroupName) {
        return dbGroups.get(dbGroupName);
    }

    private String cfg;

    public String getCfg() {
        return cfg;
    }

    private final ZKCache zkCache;

    public ZKCache getZKCache() {
        return zkCache;
    }

    private final SickMonitor sickMonitor;

    public SickMonitor getSickMonitor() {
        return sickMonitor;
    }

    public boolean isDalGroupInitialized() {
        return this.getHomeDbGroup().isInitialized();
    }

    public void setDalConfInitialized(boolean dalConfInitialized) {
        if (dalConfInitialized) {
            this.dalConfInitialized.compareAndSet(false, true);
        }
    }

    public DBChannelDispatcher(DALGroup dalGroup, ZKCache zkCache) {
        this.cfg = dalGroup.getName();
        this.schedulerGroup = new SchedulerGroup(this.cfg, zkCache);
        this.dalGroup = dalGroup;
        DBGroup homeDBGroup =
            new DBGroup(dalGroup.getDbGroup(), zkCache, this.schedulerGroup, dalGroup);
        homeDBGroup.init();
        this.dbGroups.put(dalGroup.getDbGroup(), homeDBGroup);
        this.zkCache = zkCache;
        this.sickMonitor = new SickMonitor(this.cfg);
    }

    public DBChannelDispatcher(DalGroupConfig dalGroupConfig, DALGroup dalgroup, ZKCache zkCache) {
        this(dalgroup, zkCache);
        this.dalGroupConfig = dalGroupConfig;
    }

    public String getHomeDBMasterEZone() {
        try {
            final String homeDBMaster = getHomeDbGroup().getDBServer(DBRole.MASTER);
            Objects.requireNonNull(homeDBMaster, "no active home master for " + dalGroup.getName());
            return getSched(homeDBMaster).getInfo().getEzone();
        } catch (Exception e) {
            TraceEnhancer.newFluentEvent(EventTypes.MASTER_EZONE, dalGroup.getName()).status(e)
                .complete();
            logger.error("query master ezone of " + dalGroup.getName() + " failure", e);
            return Constants.UNKNOWN;
        }
    }

    private void updateShardingCfg(DalGroupConfig dalGroupConfig) {
        ShardingRouter.updateShardRouter(dalGroupConfig);
        shardingRouter = ShardingRouter.getShardingRouter(dalGroupConfig.getName());
        shardingDb = shardingRouter.getAllShardingDbGroups();
    }

    synchronized public void updateDbInfo(List<DBConnectionInfo> infos) {
        final List<DBGroup> removeDbGroups = new ArrayList<>();
        this.dbGroups.values().stream().filter(g -> !isInScope(g.getGroupName()))
            .forEach(g -> removeDbGroups.add(g));
        removeDbGroups.forEach(removeDbGroup -> {
            removeDbGroup.getDbGroupUpdater().updateByDBCfgs(new ArrayList<>());
            this.dbGroups.remove(removeDbGroup.getGroupName());
        });

        Set<String> dbGroupNames = new HashSet<String>();
        for (DBConnectionInfo info : infos) {
            if (isInScope(info.getGroup())) {
                dbGroupNames.add(info.getGroup());
            }
        }
        for (String dbGroupName : dbGroupNames) {
            this.dbGroups.computeIfAbsent(dbGroupName, name -> {
                DBGroup dbGroup = new DBGroup(name, zkCache, this.schedulerGroup, dalGroup);
                dbGroup.init();
                return dbGroup;
            });
        }
        this.dbGroups.forEach((name, group) -> {
            group.getDbGroupUpdater().updateByDBCfgs(infos);
        });
        WeakMasterFilter.update(this);
        //only send dalgroup change info when dbgroup was removed. dbgroup modification was handled in DBConnInfoUpdater
        if (!removeDbGroups.isEmpty()) {
            MetricFactory.newCounter(Metrics.DALGROUP_CHANGE)
                .addTag(TraceNames.DALGROUP, this.dalGroup.getName())
                .addTag(TraceNames.OPERATION, DalGroupOperationType.MODIFY_DB.toString()).once();
        }
    }

    public Scheduler getSched(String id) throws QuitException {
        return schedulerGroup.getScheduler(id);
    }

    public List<Scheduler> getScheds() {
        return this.schedulerGroup.getSchedulers();
    }

    synchronized public Map<String, DBGroup> getDbGroups() {
        return new TreeMap<>(this.dbGroups);
    }

    public static void updateDispatchers(Set<String> orgs, Properties config) {
        updateTempDalGroupCfgs(orgs);
    }

    public static void updateDispatcher(DalGroupConfig dalGroupConfig) {
        synchronized (holders) {
            if (holders.containsKey(dalGroupConfig.getName())) {
                DBChannelDispatcher holder = holders.get(dalGroupConfig.getName());
                holder.updateDalGroupCfg(dalGroupConfig);
                DAL_GROUP_CONFIGS.put(dalGroupConfig.getName(), dalGroupConfig);
                return;
            }
            initDalGroupCfgDispatcher(dalGroupConfig);
            DAL_GROUP_CONFIGS.put(dalGroupConfig.getName(), dalGroupConfig);
        }
    }

    private static void initDalGroupCfgDispatcher(DalGroupConfig dalGroupConfig) {
        ZKCache zkCache = new ZKCache(dalGroupConfig.getName());
        zkCache.init(AthenaConfig.getInstance().getConfig());
        DALGroup dalGroup =
            new DALGroup(dalGroupConfig.getName(), "", dalGroupConfig.getHomeDbGroupName());
        dalGroup.setBatchAllowed(dalGroupConfig.getBatchAllowed());
        dalGroup.setPureSlaveOnly(dalGroupConfig.isPureSlaveOnly());
        dalGroup.setSlaveSelectStrategy(dalGroupConfig.getSlaveSelectStrategy());
        DBChannelDispatcher dbChannelDispatcher =
            new DBChannelDispatcher(dalGroupConfig, dalGroup, zkCache);
        holders.put(dalGroupConfig.getName(), dbChannelDispatcher);
        dbChannelDispatcher.refreshDalGroupCfg();
        ZkCuratorUtil.initZKCache(dbChannelDispatcher);
    }

    private void updateDalGroupCfg(DalGroupConfig dalGroupConfig) {
        DALGroup dalGroup =
            new DALGroup(dalGroupConfig.getName(), "", dalGroupConfig.getHomeDbGroupName());
        dalGroup.setBatchAllowed(dalGroupConfig.getBatchAllowed());
        dalGroup.setPureSlaveOnly(dalGroupConfig.isPureSlaveOnly());
        dalGroup.setSlaveSelectStrategy(dalGroupConfig.getSlaveSelectStrategy());
        this.dalGroup = dalGroup;
        if (!this.dalGroupConfig.getHomeDbGroupName().equals(dalGroupConfig.getHomeDbGroupName())) {
            this.dbGroups.forEach((dbGroupName, dbGroup) -> dbGroup.setDalGroup(this.dalGroup));
            DBGroup homeDBGroup =
                new DBGroup(dalGroupConfig.getHomeDbGroupName(), zkCache, this.schedulerGroup,
                    this.dalGroup);
            homeDBGroup.init();
            this.dbGroups.put(dalGroupConfig.getHomeDbGroupName(), homeDBGroup);
        }
        this.dalGroupConfig = dalGroupConfig;
        refreshDalGroupCfg();
    }

    private void refreshDalGroupCfg() {
        updateShardingCfg(dalGroupConfig);
        updateDbInfo(dalGroupConfig.getDbInfos());
        GlobalId.updateGlobalId(dalGroupConfig);
    }

    public static Map<String, DBChannelDispatcher> getHolders() {
        return holders;
    }

    public boolean enqueue(SqlSessionContext sqlCtx) {
        SchedulerWorker.getInstance().enqueue("DBChannelDispatcher.enqueue", sqlCtx);
        return true;
    }

    public boolean isInScope(String dbGroup) {
        if (dbGroup == null) {
            return false;
        }
        if (dbGroup.equals(this.dalGroup.getDbGroup())) {
            return true;
        }
        return shardingDb.contains(dbGroup);
    }

    public boolean isOnline(DBConnectionInfo info) {
        if (info == null) {
            return false;
        }

        if (!isInScope(info.getGroup())) {
            return false;
        }
        return !this.zkCache.getOfflineDbs().contains(info.getQualifiedDbId());
    }

    public ShardingRouter getShardingRouter() {
        return shardingRouter;
    }

    public boolean isBatchAllowed() {
        return dalGroup.isBatchAllowed();
    }

    public void updateSQLTrafficPolicing(Map<String, Double> tps) {
        this.sqlTrafficPolicings.keySet().removeIf(k -> !tps.containsKey(k));
        Map<String, Double> newtps = new HashMap<>();
        tps.forEach((sqlid, rate) -> {
            SQLTrafficPolicing tp = sqlTrafficPolicings.get(sqlid);
            if (tp == null || Double.compare(rate, tp.rate) != 0) {
                newtps.put(sqlid, rate);
            }
        });
        newtps.forEach(
            (sqlid, rate) -> sqlTrafficPolicings.put(sqlid, new SQLTrafficPolicing(sqlid, rate)));
        newtps.forEach((sqlid, rate) -> killBySqlid(sqlid));
        logger.info("updateSQLTrafficPolicing " + tps);
    }

    /**
     * 手动查杀当前dalgroup上匹配sqlid的SQL
     *
     * @param sqlid 可以设置为"*", 表示当前dalgroup上所有sql
     */
    public void killBySqlid(String sqlid) {
        NoThrow.call(() -> {
            Predicate<SqlSessionContext> isTargetSqlidSession = s -> {
                final CmdQuery cmdQuery = s.curCmdQuery;
                return s.getHolder() == DBChannelDispatcher.this && cmdQuery != null && (
                    "*".equals(sqlid) || sqlid.equals(cmdQuery.getSqlId()));
            };
            HangSessionMonitor.getAllCtx().stream().filter(isTargetSqlidSession).forEach(s -> {
                SchedulerWorker.getInstance().enqueue(
                    new ManualKillerManager(s, SessionQuitTracer.QuitTrace.ManualKill,
                        ErrorCode.ABORT_MANUAL_KILL, "connection is killed by admin"));
            });
        });
        logger.warn("[{}] kill sqlid [{}]", this.dalGroup.getName(), sqlid);
    }

    public SQLTrafficPolicing getSqlTrafficPolicing(String sqlid) {
        if (sqlTrafficPolicings.keySet().contains("*")) {
            return sqlTrafficPolicings.get("*");
        }
        return this.sqlTrafficPolicings.get(sqlid);
    }

    public boolean isOffline() {
        return this.dalGroup.isOffline();
    }

    /**
     * handles dalgroup offline operation from ZK
     */
    public void offline() {
        logger.error("Set dalgroup {} to offline", this.cfg);
        this.dalGroup.setOffline(true);
        //delete the db connection from offlined dalgroup
        this.dalGroupConfig.deleteFile();
    }

    public void clearResources() {
        this.dalGroup.setOffline(true);
        this.dalGroupConfig.setDalgroupOffline(true);
        for (DBGroup dbGroup : this.dbGroups.values()) {
            dbGroup.getDbGroupUpdater().updateByDBCfgs(Collections.emptyList());
            dbGroup.getDbGroupUpdater().stopWindow();
        }
        this.dbGroups.clear();
    }

    private static void updateTempDalGroupCfgs(Set<String> orgs) {
        synchronized (TEMP_DAL_GROUP_CONFIGS) {
            final Set<String> orgdalGroups = new HashSet<>();
            AthenaConfig.getInstance().getDbCfgs().forEach(org -> {
                OrgConfig.getInstance().getDalGroupsByOrg(org).forEach(dalgroup -> {
                    orgdalGroups.add(dalgroup.getName());
                });
            });
            orgs.forEach(org -> {
                List<DALGroup> dalgroups = OrgConfig.getInstance().getDalGroupsByOrg(org);
                if (dalgroups.isEmpty() && !orgdalGroups.contains(org)) {
                    // add a default dalgroup
                    dalgroups = new ArrayList<>();
                    dalgroups.add(new DALGroup(org, org, org));
                }
                dalgroups.forEach(group -> {
                    if (TEMP_DAL_GROUP_CONFIGS.containsKey(group.getName())) {
                        group.setOffline(false);
                        DalGroupConfig oldConfig = TEMP_DAL_GROUP_CONFIGS.get(group.getName());
                        oldConfig.updateByDalgroup(group);
                        return;
                    }
                    DalGroupConfig dalGroupConfig = new DalGroupConfig();
                    dalGroupConfig.updateConfigs(group);
                    logger.info("Generating dalgroup config for dalgroup {}", group.getName());
                    TEMP_DAL_GROUP_CONFIGS.put(group.getName(), dalGroupConfig);
                });
            });
            //offline deleted dalgroups
            TEMP_DAL_GROUP_CONFIGS.entrySet().stream().filter(
                entry -> !orgdalGroups.contains(entry.getKey()) && !Constants.DUMMY
                    .equals(entry.getKey())).forEach(entry -> {
                logger.error("Set dalgroup {} to offline", entry.getKey());
                entry.getValue().setDalgroupOffline(true);
                entry.getValue().deleteFile();
            });
        }
    }

    public DalGroupConfig getDalGroupConfig() {
        return dalGroupConfig;
    }

}
