package me.ele.jarch.athena.util.health.check;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.etrace.agent.Trace;
import me.ele.jarch.athena.allinone.DBConnectionInfo;
import me.ele.jarch.athena.allinone.DBGroup;
import me.ele.jarch.athena.allinone.DBVendor;
import me.ele.jarch.athena.constant.Metrics;
import me.ele.jarch.athena.constant.TraceNames;
import me.ele.jarch.athena.netty.AthenaServer;
import me.ele.jarch.athena.scheduler.DBChannelDispatcher;
import me.ele.jarch.athena.server.async.AsyncClient;
import me.ele.jarch.athena.util.AthenaConfig;
import me.ele.jarch.athena.util.GreySwitch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static java.lang.System.currentTimeMillis;
import static me.ele.jarch.athena.constant.Constants.ATHENA_ADMIN_READONLY;

public class DalGroupHealthCheck {
    private static final Logger logger = LoggerFactory.getLogger(DalGroupHealthCheck.class);

    static final long TIMEOUT = 10_000;
    static final int RETRY_COUNT = 3;

    static final Cache<String, DalGroupHealthCheckStatus> HEALTH_CHECK_RESULT =
        CacheBuilder.newBuilder().expireAfterWrite(4, TimeUnit.MINUTES).build();


    private static final String QUERY_SHARD_TABLE_SQL =
        "SELECT 1 FROM %s where 'DAL_HEALTH_CHECK' = 'CHECK_HEALTH_DAL' AND sharding_index=%d LIMIT 1";
    private static final String QUERY_MAIN_DB_SQL = "SELECT 1";


    private static class INNER {
        static DalGroupHealthCheck dalGroupHealthCheck = new DalGroupHealthCheck();
    }

    public static DalGroupHealthCheck getInstance() {
        return INNER.dalGroupHealthCheck;
    }

    public void check() {
        //Setup health check job for next round
        AthenaServer.dalGroupHealthCheckJobScheduler.addOneTimeJob("dal_group_health_check",
            GreySwitch.getInstance().getHealthCheckInterval(),
            DalGroupHealthCheck.getInstance()::check);

        if (!GreySwitch.getInstance().isAllowDalGroupHealthCheck()) {
            return;
        }
        AtomicLong initialDalay = new AtomicLong(0);
        DBChannelDispatcher.getHolders().forEach((dalGroup, dispatcher) -> {
            if (dispatcher.isOffline()) {
                logger.info("Skip dalgroup health check for  group {} as it is offline", dalGroup);
                return;
            }
            boolean isMysql = isMysqlDB(dalGroup);
            boolean hasMaster =
                isMasterConfigured(dispatcher, dispatcher.getHomeDbGroup().getGroupName());
            AthenaServer.dalGroupHealthCheckJobScheduler
                .addOneTimeJob("dalGroup_health_check_job", initialDalay.getAndAdd(10), () -> {
                    logger.info("dalGroup health check start, dalGroup is: {}", dalGroup);
                    Optional<Integer> dalPort = tryGetDalPort(dalGroup, isMysql);
                    if (!dalPort.isPresent()) {
                        return;
                    }
                    // check slave
                    ArrayList<HealthCheckItem> slaveHealthCheckItems =
                        parseHealthCheckItems(dalGroup, dispatcher, false);
                    doHealthCheck(dalGroup, slaveHealthCheckItems, dalPort.get(), isMysql);

                    // check master
                    if (hasMaster) {
                        ArrayList<HealthCheckItem> masterHealthCheckItems =
                            parseHealthCheckItems(dalGroup, dispatcher, true);
                        doHealthCheck(dalGroup, masterHealthCheckItems, dalPort.get() + 1, isMysql);
                    }

                });
        });
        printEtraceMetrics();
    }

    private DalGroupHealthCheckStatus getFailedStatus(boolean isMaster, boolean isShardDb) {
        if (isMaster) {
            if (isShardDb) {
                return DalGroupHealthCheckStatus.SHARDING_MASTER_HEART_BEAT_FAILED;
            } else {
                return DalGroupHealthCheckStatus.MASTER_HEART_BEAT_FAILED;
            }
        } else {
            if (isShardDb) {
                return DalGroupHealthCheckStatus.SHARDING_SLAVE_HEART_BEAT_FAILED;
            } else {
                return DalGroupHealthCheckStatus.SLAVE_HEART_BEAT_FAILED;
            }
        }
    }

    private ArrayList<HealthCheckItem> parseHealthCheckItems(String dalGroup,
        DBChannelDispatcher dispatcher, boolean isMaster) {
        String roleSuffix = isMaster ? ".master" : ".slave";
        ArrayList<HealthCheckItem> healthCheckItems = new ArrayList<>();
        // add home db sql
        DalGroupHealthCheckStatus homeFailStatus = getFailedStatus(isMaster, false);
        HealthCheckItem homeItem =
            new HealthCheckItem(dalGroup + roleSuffix, QUERY_MAIN_DB_SQL, homeFailStatus);
        healthCheckItems.add(homeItem);

        // skip sharding table health check if it shares sharding cfg with parent dalgroup
        if (dispatcher.getCfg().contains("__sub__")) {
            return healthCheckItems;
        }
        // perform health check for sharding dbs by send 'select 1 from shard_table where sharding_index = ?',
        // pick only first shard of each db group
        DalGroupHealthCheckStatus shardFailStatus = getFailedStatus(isMaster, true);
        dispatcher.getShardingRouter().getHealthCheckShardIdx().forEach((pairTableKey, indexs) -> {
            indexs.forEach((dbGroup, index) -> {
                String shardTag = dalGroup + roleSuffix + "." + pairTableKey.table + "#" + index;
                String querySql = String.format(QUERY_SHARD_TABLE_SQL, pairTableKey.table, index);
                healthCheckItems.add(new HealthCheckItem(shardTag, querySql, shardFailStatus));
            });
        });

        return healthCheckItems;
    }

    private Optional<Integer> tryGetDalPort(String dalGroupName, boolean isMysql) {
        int[] ports = isMysql ?
            AthenaConfig.getInstance().getServicePorts() :
            AthenaConfig.getInstance().getPgPort();
        if (ports.length == 0) {
            logger.error("dalGroup {} has no service port to connect", dalGroupName);
            HEALTH_CHECK_RESULT.put(dalGroupName, DalGroupHealthCheckStatus.NO_SERVICE_PORT);
            return Optional.empty();
        }
        return Optional.of(ports[0]);
    }

    /**
     * create healthCheckClient for check both home db and shard db
     */
    private void doHealthCheck(String dalGroupName, ArrayList<HealthCheckItem> healthCheckItems,
        int dalPort, boolean isMysql) {
        DBConnectionInfo dbConnectionInfo =
            buildDalGroupDBConn(dalGroupName, dalPort, healthCheckItems.get(0), isMysql);
        AsyncClient client = isMysql ?
            newMysqlHCClient(dbConnectionInfo, healthCheckItems) :
            newPGHCClient(dbConnectionInfo, healthCheckItems);
        client.doAsyncExecute();
    }

    public Map<String, DalGroupHealthCheckStatus> getUnhealthyDalGroup() {
        return HEALTH_CHECK_RESULT.asMap().entrySet().stream().filter(
            e -> !e.getKey().contains("#") && !e.getValue()
                .equals(DalGroupHealthCheckStatus.HEART_BEAT_SUCCESS))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }


    public void printEtraceMetrics() {
        HEALTH_CHECK_RESULT.asMap().entrySet().stream().filter(e -> !e.getKey().contains("#"))
            .forEach(e -> Trace.newCounter(Metrics.DALGROUP_HEALTH_CHECK)
                .addTag(TraceNames.DALGROUP, e.getKey())
                .addTag(TraceNames.STATUS, e.getValue().toString()).once());

        getShardTableStatus().forEach(
            (dalGroup, status) -> Trace.newCounter(Metrics.DALGROUP_HEALTH_CHECK)
                .addTag(TraceNames.DALGROUP, dalGroup).addTag(TraceNames.STATUS, status.toString())
                .once());
    }

    public TraceHealthResponse getCheckResult() {
        boolean success = HEALTH_CHECK_RESULT.asMap().values().stream().distinct()
            .allMatch(stat -> stat.equals(DalGroupHealthCheckStatus.HEART_BEAT_SUCCESS));
        Map<String, DalGroupIndicatorInfo> indicators = new LinkedHashMap<>();
        HEALTH_CHECK_RESULT.asMap().entrySet().stream().filter(e -> !e.getKey().contains("#"))
            .forEach(e -> {
                indicators
                    .put(e.getKey(), new DalGroupIndicatorInfo(false, e.getValue().toString()));
            });
        getShardTableStatus().forEach((shardtable, status) -> indicators
            .put(shardtable, new DalGroupIndicatorInfo(false, status.toString())));
        return new TraceHealthResponse(success, currentTimeMillis(), "单点检测", indicators);
    }

    private boolean isMysqlDB(String dalGroup) {
        return !dalGroup.contains("pggroup");
    }

    private boolean isMasterConfigured(DBChannelDispatcher dispatcher, String dbGroup) {
        DBGroup dbGroupCfg = dispatcher.getDbGroup(dbGroup);
        if (Objects.isNull(dbGroupCfg)) {
            return false;
        }
        return dispatcher.getDbGroup(dbGroup).getDbGroupUpdater().isMasterConfigured();
    }

    private DBConnectionInfo buildDalGroupDBConn(String dalGroup, int port,
        HealthCheckItem homeItem, boolean isMysql) {
        DBConnectionInfo dbConnectionInfo = new DBConnectionInfo();
        dbConnectionInfo.setDatabase(dalGroup);
        dbConnectionInfo.setHost(AthenaConfig.getInstance().getLoopbackIP());
        dbConnectionInfo.setPort(port);
        dbConnectionInfo.setUser(ATHENA_ADMIN_READONLY);
        dbConnectionInfo.setPword(AthenaConfig.getInstance().getDalAdminReadonlyPwd());
        dbConnectionInfo.setId(homeItem.getTag());
        if (!isMysql) {
            dbConnectionInfo.setDBVendor(DBVendor.PG);
        }
        return dbConnectionInfo;
    }

    protected MysqlHealthCheckClient newMysqlHCClient(DBConnectionInfo dalGroupSlaveDBConn,
        ArrayList<HealthCheckItem> healthCheckItems) {
        MysqlHealthCheckResponseHandler healthCheckHandler =
            new MysqlHealthCheckResponseHandler(healthCheckItems);
        MysqlHealthCheckClient healthCheckClient =
            new MysqlHealthCheckClient(dalGroupSlaveDBConn, healthCheckHandler, TIMEOUT);
        healthCheckClient.setSql(healthCheckItems.get(0).getSql());
        healthCheckHandler.setHealthCheckClient(healthCheckClient);
        return healthCheckClient;
    }

    protected PGHealthCheckClient newPGHCClient(DBConnectionInfo dalGroupSlaveDBConn,
        ArrayList<HealthCheckItem> healthCheckItems) {
        PGHealthCheckResponseHandler healthCheckHandler =
            new PGHealthCheckResponseHandler(healthCheckItems);
        PGHealthCheckClient healthCheckClient =
            new PGHealthCheckClient(dalGroupSlaveDBConn, healthCheckHandler, TIMEOUT);
        healthCheckClient.setSql(healthCheckItems.get(0).getSql());
        healthCheckHandler.setHealthCheckClient(healthCheckClient);
        return healthCheckClient;
    }

    /**
     * merge shard table status into one by comparing the ordinal of DalGroupHealthCheckStatus per below priority
     * SHARDING_MASTER_HEART_BEAT_FAILED < SHARDING_SLAVE_HEART_BEAT_FAILED < NO_SERVICE_PORT
     *
     * @return merged shardTable status map
     */
    public Map<String, DalGroupHealthCheckStatus> getShardTableStatus() {
        HashMap<String, DalGroupHealthCheckStatus> shardDBStatus = new HashMap<>();
        HEALTH_CHECK_RESULT.asMap().entrySet().stream().filter(e -> e.getKey().contains("#"))
            .forEach(e -> {
                String realShardTableName = e.getKey().split("#")[0];
                shardDBStatus.merge(realShardTableName, e.getValue(), (preStatus, curStatus) ->
                    preStatus.ordinal() < curStatus.ordinal() ?
                        curStatus :
                        preStatus);
            });
        return shardDBStatus;
    }

}
