package me.ele.jarch.athena.netty.state;

import com.github.mpjct.jmpjct.mysql.proto.Column;
import com.github.mpjct.jmpjct.mysql.proto.ERR;
import com.github.mpjct.jmpjct.mysql.proto.ResultSet;
import com.github.mpjct.jmpjct.mysql.proto.Row;
import com.github.mpjct.jmpjct.util.ErrorCode;
import io.netty.buffer.Unpooled;
import me.ele.jarch.athena.AutoreadSwitch;
import me.ele.jarch.athena.SQLLog;
import me.ele.jarch.athena.UnsafeLog;
import me.ele.jarch.athena.allinone.DBConnectionInfo;
import me.ele.jarch.athena.allinone.DBRole;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.constant.Metrics;
import me.ele.jarch.athena.constant.TraceNames;
import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.netty.SessionQuitTracer;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.scheduler.DBChannelDispatcher;
import me.ele.jarch.athena.scheduler.HangSessionMonitor;
import me.ele.jarch.athena.server.async.AsyncOneTimeClient;
import me.ele.jarch.athena.server.async.AsyncResultSetHandler;
import me.ele.jarch.athena.sharding.ShardingConfig.ComposedKey;
import me.ele.jarch.athena.sharding.ShardingConfig.MappingKey;
import me.ele.jarch.athena.sharding.ShardingRouter;
import me.ele.jarch.athena.sharding.ShardingTable;
import me.ele.jarch.athena.sharding.sql.ShardingSQL;
import me.ele.jarch.athena.sql.CmdQuery;
import me.ele.jarch.athena.sql.ResultType;
import me.ele.jarch.athena.sql.seqs.*;
import me.ele.jarch.athena.util.*;
import me.ele.jarch.athena.util.dbconfig.DbConfigInfo;
import me.ele.jarch.athena.util.etrace.EtracePatternUtil;
import me.ele.jarch.athena.util.etrace.MetricFactory;
import me.ele.jarch.athena.util.etrace.MetricMetaExtractor;
import me.ele.jarch.athena.worker.SchedulerWorker;
import me.ele.jarch.athena.worker.manager.ManualKillerManager;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class FakeSqlState implements State {

    private static final Logger logger = LoggerFactory.getLogger(FakeSqlState.class);

    protected static final String DEFAULT_RESULTSET_TITLE = "result";

    private static final String TABLE_NAME = "table_name";

    private static final String VALUE = "value";

    private static final String DAL_GROUP_NAME = "dal_group";

    // select killgroup from dal_dual where group = "eos_group";
    protected static final String KILL_GROUP = "killgroup";

    // select killctx from dal_dual where value = 123;
    private static final String KILL_CTX_PREFIX = "killctx";
    // select unsafe_on from dal_dual;
    private static final String UNSAFE_SQL_ON = "unsafe_on";
    // select unsafe_off from dal_dual;
    private static final String UNSAFE_SQL_OFF = "unsafe_off";
    // set the percentage of sending SQL pattern to etrace
    // such as:
    // select etrace_sql_pattern from dal_dual where value = 999;
    private static final String ETRACE_SQL_PATTERN_PREFIX = "etrace_sql_pattern";

    // SELECT sharded_tables FROM dal_dual where table_name = 'tb_shipping_order'
    private static final String SHARDED_TABLES = "sharded_tables";

    // SELECT shard_info FROM dal_dual WHERE table_name = 'eleme_order' AND id = '123';
    private static final String SHARD_INFO = "shard_info";

    // SELECT db_config FROM dal_dual where group = 'eos_group';
    protected static final String DB_CONFIG = "db_config";

    private static final String TABLES = "tables";

    private static final String FAKE_SQL_HELP = "help";

    private static final String SHARD_INFO_TEMPLATE =
        "SELECT shard_info FROM dal_dual WHERE table_name = '%s' AND %s = '123'";

    private static final String SHARD_INFO_TABLE_TEMPLATE =
        "SELECT shard_info FROM dal_dual WHERE table_name = '%s'";

    private static final String SHARD_INFO_COLUMN_TEMPLATE = " AND %s = '123'";

    private static Map<String, ControlFakeSql> FAKE_SQLS = new LinkedHashMap<>();

    private static final List<String> helps = new ArrayList<>();

    static {
        FAKE_SQLS.put(KILL_GROUP, new ControlFakeSql() {

            @Override public String help() {
                return String
                    .format("SELECT %s FROM dal_dual WHERE group = 'eos_group'", KILL_GROUP);
            }

            @Override public void doCtrlFakeSql(FakeSqlState status, Map<String, String> params) {
                status.doKillGroup(params);
            }
        });

        FAKE_SQLS.put(KILL_CTX_PREFIX, new ControlFakeSql() {

            @Override public String help() {
                return String.format("SELECT %s FROM dal_dual WHERE value = 123", KILL_CTX_PREFIX);
            }

            @Override public void doCtrlFakeSql(FakeSqlState status, Map<String, String> params) {
                status.doKillCtx(params);
            }
        });
        FAKE_SQLS.put(UNSAFE_SQL_ON, new ControlFakeSql() {

            @Override public String help() {
                return String.format("SELECT %s FROM dal_dual", UNSAFE_SQL_ON);
            }

            @Override public void doCtrlFakeSql(FakeSqlState status, Map<String, String> params) {
                status.doUnsafeLog(true);
            }
        });
        FAKE_SQLS.put(UNSAFE_SQL_OFF, new ControlFakeSql() {

            @Override public String help() {
                return String.format("SELECT %s FROM dal_dual", UNSAFE_SQL_OFF);
            }

            @Override public void doCtrlFakeSql(FakeSqlState status, Map<String, String> params) {
                status.doUnsafeLog(false);
            }
        });
        FAKE_SQLS.put(AutoreadSwitch.AUTOREAD_ON, new ControlFakeSql() {

            @Override public String help() {
                return String.format("SELECT %s FROM dal_dual", AutoreadSwitch.AUTOREAD_ON);
            }

            @Override public void doCtrlFakeSql(FakeSqlState status, Map<String, String> params) {
                status.doAutoreadSwitch(true);
            }
        });
        FAKE_SQLS.put(AutoreadSwitch.AUTOREAD_OFF, new ControlFakeSql() {

            @Override public String help() {
                return String.format("SELECT %s FROM dal_dual", AutoreadSwitch.AUTOREAD_OFF);
            }

            @Override public void doCtrlFakeSql(FakeSqlState status, Map<String, String> params) {
                status.doAutoreadSwitch(false);
            }
        });
        FAKE_SQLS.put(ETRACE_SQL_PATTERN_PREFIX, new ControlFakeSql() {

            @Override public String help() {
                return String
                    .format("SELECT %s FROM dal_dual WHERE value = 999", ETRACE_SQL_PATTERN_PREFIX);
            }

            @Override public void doCtrlFakeSql(FakeSqlState status, Map<String, String> params) {
                status.doETraceChange(params);
            }
        });
        FAKE_SQLS.put(SHARDED_TABLES, new ControlFakeSql() {

            @Override public String help() {
                return String.format("SELECT %s FROM dal_dual where %s = 'tb_shipping_order'",
                    SHARDED_TABLES, TABLE_NAME);
            }

            @Override public void doCtrlFakeSql(FakeSqlState status, Map<String, String> params) {
                status.doShardedTables(params);
            }

        });
        FAKE_SQLS.put(SHARD_INFO, new ControlFakeSql() {
            @Override public String help() {
                return String.format(
                    "SELECT %s FROM dal_dual WHERE %s = 'tb_shipping_order' AND tracking_id = '123'",
                    SHARD_INFO, TABLE_NAME);
            }

            @Override public void doCtrlFakeSql(FakeSqlState status, Map<String, String> params) {
                status.doShardInfo(params);
            }
        });
        FAKE_SQLS.put(DB_CONFIG, new ControlFakeSql() {

            @Override public String help() {
                return String
                    .format("SELECT %s FROM dal_dual where group = 'eos_group'", DB_CONFIG);
            }

            @Override public void doCtrlFakeSql(FakeSqlState status, Map<String, String> params) {
                status.doQueryDbConfig(params);
            }
        });
        FAKE_SQLS.put(TABLES, new ControlFakeSql() {

            @Override public String help() {
                return String.format("SELECT %s FROM dal_dual", TABLES);
            }

            @Override public void doCtrlFakeSql(FakeSqlState status, Map<String, String> params) {
                status.doQueryShowTables();
            }
        });
        FAKE_SQLS.put(FAKE_SQL_HELP, new ControlFakeSql() {

            @Override public String help() {
                return String.format("SELECT %s FROM dal_dual", FAKE_SQL_HELP);
            }

            @Override public void doCtrlFakeSql(FakeSqlState status, Map<String, String> params) {
                status.doHelp();
            }
        });
        FAKE_SQLS.values().stream().map(x -> x.help()).forEach(x -> helps.add(x));
        helps.add(
            "SELECT next_value FROM dal_dual WHERE seq_name='order_id' AND user_id='123' AND restaurant_id='456'");
        helps.add(String
            .format("SELECT sharding_count FROM dal_dual where %s = 'eleme_order'", TABLE_NAME));
        helps.add(
            "SELECT next_value FROM dal_dual WHERE seq_name = 'common_seq' AND biz = 'hongbao'");
        helps.add(
            "SELECT next_value FROM dal_dual WHERE seq_name = 'composed_seq' AND biz = 'delivery_order' AND org_id = '123' AND team_id = '456'");
    }


    public static abstract class ControlFakeSql {
        public abstract String help();

        public abstract void doCtrlFakeSql(FakeSqlState status, Map<String, String> params);
    }

    public static boolean isGetGlobalIdSQL(CmdQuery query) {
        if (Objects.isNull(query)) {
            return false;
        }
        if (Objects.isNull(query.shardingSql)) {
            return false;
        }
        if (!GeneratorUtil.DAL_DUAL.equalsIgnoreCase(query.table)) {
            return false;
        }
        if (GeneratorUtil.NEXT_BEGIN.equalsIgnoreCase(query.shardingSql.next_begin)
            && GeneratorUtil.NEXT_END.equalsIgnoreCase(query.shardingSql.next_end)) {
            return true;
        }

        return GeneratorUtil.NEXT_VALUE.equalsIgnoreCase(query.shardingSql.selected_value);
    }

    public static boolean isFakeSQL(CmdQuery query) {
        if (Objects.isNull(query)) {
            return false;
        }
        if (Objects.isNull(query.shardingSql)) {
            return false;
        }
        return FAKE_SQLS.containsKey(query.shardingSql.selected_value);
    }

    protected final SqlSessionContext sqlSessionContext;

    public FakeSqlState(SqlSessionContext sqlSessionContext) {
        this.sqlSessionContext = sqlSessionContext;
    }

    @Override public boolean handle() throws QuitException {
        doFakeSql();
        return false;
    }

    @Override public SESSION_STATUS getStatus() {
        return SESSION_STATUS.FAKE_SQL;
    }

    private void doFakeSql() throws QuitException {
        sqlSessionContext.sqlSessionContextUtil.resetQueryTimeRecord();
        sqlSessionContext.setState(SESSION_STATUS.QUERY_ANALYZE);
        if (Objects.isNull(sqlSessionContext.curCmdQuery)) {
            write2Client("strange status,cannot get sqlSessionContext.curCmdQuery.shardingSql");
            return;
        }
        ShardingSQL shardingSQL = sqlSessionContext.curCmdQuery.shardingSql;
        if (Objects.isNull(shardingSQL)) {
            write2Client("strange status,cannot get sqlSessionContext.curCmdQuery.shardingSql");
            return;
        }

        if (tryExecuteControlFakeSql(shardingSQL)) {
            SQLLog.log(sqlSessionContext.curCmdQuery, sqlSessionContext.sessInfo(),
                sqlSessionContext.getClientInfo(), null, -1, -1, null, null);
            sqlSessionContext.curCmdQuery = null;
            // 结束etrace对非sharding相关fakesql的追踪
            sqlSessionContext.sqlSessionContextUtil.endEtrace();
            return;
        }
        if (sqlSessionContext.seqsParse == null) {
            return;
        }
        try {
            sqlSessionContext.sqlSessionContextUtil.startGlobalIdEtrace();
            doFakeGlobalIDSQL(sqlSessionContext.seqsParse);
        } catch (QuitException t) {
            sqlSessionContext.quitTracer.reportQuit(SessionQuitTracer.QuitTrace.GlobalIdErr);
            logger.error(String
                .format("exception caught, close channel on [%s] [%s]", this.toString(),
                    sqlSessionContext.toString()), t);
            sqlSessionContext.kill(ErrorCode.ABORT_GLOBAL_ID, "GlobalID db connection abort");
        }
    }

    private void doHelp() {
        String modernTemplate =
            "SELECT next_value FROM dal_dual WHERE seq_name = '%s' AND biz = '%s'";
        String legacyTemplate = "SELECT next_value FROM dal_dual WHERE seq_name = '%s'";
        String embeddedConditionTemplate = " AND %s = '%s'";
        List<String> moreHelps = new ArrayList<>();
        ShardingRouter router = this.sqlSessionContext.getShardingRouter();
        List<GlobalId> globalIds =
            GlobalId.getGlobalIdsByDALGroup(sqlSessionContext.getHolder().getDalGroup().getName());
        if (!globalIds.isEmpty()) {
            moreHelps
                .add("------------------------------------------------------------------------");
            moreHelps.add("The following are active globalIds in this dalgroup");
            moreHelps.add("'123' just is an example, you can replace it with you business value");
        }
        for (GlobalId globalId : globalIds) {
            final StringBuilder msg = new StringBuilder();
            msg.append(String.format(modernTemplate, globalId.generator, globalId.bizName));
            globalId.params.forEach(
                param -> msg.append(String.format(embeddedConditionTemplate, param, "123")));
            moreHelps.add(msg.toString());
        }
        List<String> msgList = new LinkedList<>(helps);
        msgList.addAll(doDynamicHelps());
        msgList.addAll(moreHelps);
        msgList.addAll(buildShardInfoHelp(router));
        write2Client(DEFAULT_RESULTSET_TITLE, msgList);
    }

    protected List<String> doDynamicHelps() {
        List<String> dynamicHelps = new LinkedList<>();
        dynamicHelps
            .add("------------------------------------------------------------------------");
        dynamicHelps.add("The following help SQLs suit for current protocol");
        dynamicHelps.add(String.format("SELECT %s FROM dal_dual where group = '%s'", DB_CONFIG,
            sqlSessionContext.getHolder().getDalGroup().getName()));
        dynamicHelps
            .add(String.format("SELECT %s FROM dal_dual WHERE group = 'eos_group'", KILL_GROUP));
        return dynamicHelps;
    }

    private Collection<String> buildShardInfoHelp(ShardingRouter router) {
        Set<String> shardInfoHelps = new LinkedHashSet<>();
        Set<String> tables = router.getAllOriginShardedTables();
        if (!tables.isEmpty()) {
            shardInfoHelps
                .add("------------------------------------------------------------------------");
            shardInfoHelps
                .add("The following sql is sample sql for query shard info by sharding columns");
        }
        for (String table : tables) {
            ComposedKey ck = router.getComposedKey(table);
            if (ck.getColumn().equals(ck.getSeq_name())) {
                // compose_key的column == seq_name 或者 seq_name 是ShardidDecoder系列都值显示column相关的帮助信息
                shardInfoHelps.addAll(
                    buildShardingInfoSamplesByShardingColumns(ck.getColumns(), table,
                        SHARD_INFO_TEMPLATE));
            } else if ("mapping".equals(ck.getSeq_name())) {
                // 查询mapping表的shard info
                shardInfoHelps
                    .add(buildShardingInfoSamplesByMultiMappingColumns(ck.getColumns(), table));
            } else if (StringUtils.isEmpty(ck.getColumn())) {
                // 如果没有composed_key的情况下,只显示shardingKey的help信息
                shardInfoHelps.addAll(
                    buildShardingInfoSamplesByShardingColumns(ck.getShardingColumns(), table,
                        SHARD_INFO_TEMPLATE));
            } else {
                shardInfoHelps.addAll(
                    buildShardingInfoSamplesByShardingColumns(ck.getColumns(), table,
                        SHARD_INFO_TEMPLATE));
                shardInfoHelps.addAll(
                    buildShardingInfoSamplesByShardingColumns(ck.getShardingColumns(), table,
                        SHARD_INFO_TEMPLATE));
            }
            if (router.isMappingShardingTable(table)) {
                //根据mapping查询shard info
                router.getMappingKey(table).forEach((column, mk) -> {
                    // 如果mapping key有多个column, 分别显示多个mapping字段
                    shardInfoHelps.addAll(
                        buildShardingInfoSamplesByMappingColumns(mk.getMappingColumns(), table));
                });
            }
        }
        return shardInfoHelps;
    }

    /**
     * 通过sharding相关列, 构造sharding info 的查询SQL样例
     *
     * @param shardingColumns sharding相关列,可以是composeKey, shardingKey, mappingKey
     * @param table           sharding表名
     * @param template        样例模板
     * @return 构造的所有样例SQL
     */
    private static List<String> buildShardingInfoSamplesByShardingColumns(
        final List<String> shardingColumns, final String table, final String template) {
        return shardingColumns.stream().filter(StringUtils::isNotEmpty)
            .map(shardingKey -> String.format(template, table, shardingKey))
            .collect(Collectors.toList());
    }

    private static String buildShardingInfoSamplesByMultiMappingColumns(
        final List<String> mappingColumns, final String table) {
        String embeddedConditionTemplate = " AND %s = '123'";
        StringBuilder sampleString = new StringBuilder();
        sampleString.append(
            String.format("SELECT shard_info FROM dal_dual WHERE table_name = '%s'", table));
        mappingColumns.stream().filter(StringUtils::isNotEmpty).forEach(
            mappingColumn -> sampleString
                .append(String.format(embeddedConditionTemplate, mappingColumn)));
        return sampleString.toString();
    }

    private static List<String> buildShardingInfoSamplesByMappingColumns(
        final Map<String, List<String>> shardingColumns, final String table) {
        return shardingColumns.values().stream().filter(v -> !v.isEmpty()).map(columns -> {
            StringBuffer sql = new StringBuffer(String.format(SHARD_INFO_TABLE_TEMPLATE, table));
            for (String column : columns) {
                sql.append(String.format(SHARD_INFO_COLUMN_TEMPLATE, column));
            }
            return sql.toString();
        }).collect(Collectors.toList());
    }

    private void doKillCtx(Map<String, String> params) {
        try {
            long connId = Long.valueOf(params.getOrDefault(VALUE, "-1"));
            if (connId == -1) {
                logger.error("no value = ? in " + KILL_CTX_PREFIX);
                write2Client("no value = ? in " + KILL_CTX_PREFIX);
                return;
            }
            Optional<SqlSessionContext> find =
                HangSessionMonitor.getAllCtx().stream().filter(ctx -> ctx.connectionId == connId)
                    .findAny();
            if (find.isPresent()) {
                SchedulerWorker.getInstance().enqueue(
                    new ManualKillerManager(find.get(), SessionQuitTracer.QuitTrace.ManualKill,
                        ErrorCode.ABORT_MANUAL_KILL, "connection is killed by admin"));
                logger.warn("clientConnId=" + connId + " has killed");
                write2Client(connId + " has killed");
            } else {
                logger.warn("cannot find clientConnId=" + connId);
                write2Client("cannot find clientConnId=" + connId);
            }
        } catch (Exception e) {
            logger
                .error(KILL_CTX_PREFIX + " failed to execute : " + sqlSessionContext.getCurQuery(),
                    e);
            write2Client("invalid command: no value = ? in " + KILL_CTX_PREFIX);
        }
    }

    private void doKillGroup(Map<String, String> params) {
        try {
            if (!Constants.ATHENA_ADMIN.equals(this.sqlSessionContext.authenticator.userName)) {
                write2Client("Access denied");
                return;
            }
            String dalgroup = params.getOrDefault("group", "");
            DBChannelDispatcher dispatcher = DBChannelDispatcher.getHolders().get(dalgroup);
            if (dispatcher == null) {
                logger.error("cannot find dalgroup for  " + KILL_GROUP + " " + sqlSessionContext);
                write2Client("cannot find dalgroup " + KILL_GROUP);
                return;
            }
            HangSessionMonitor.getAllCtx().stream().filter(
                s -> s.getHolder() != null && dalgroup.equals(s.getHolder().getCfg())
                    && !Constants.ATHENA_ADMIN.equals(s.authenticator.userName)).forEach(s -> {
                SchedulerWorker.getInstance().enqueue(
                    new ManualKillerManager(s, SessionQuitTracer.QuitTrace.ManualKill,
                        ErrorCode.ABORT_MANUAL_KILL, "connection is killed by admin"));
            });
            write2Client(dalgroup + " has killed");
        } catch (Exception e) {
            logger.error(KILL_GROUP + " failed to execute : " + sqlSessionContext, e);
            write2Client("failed to execute " + KILL_GROUP);
        }

    }

    private void doETraceChange(Map<String, String> params) {
        try {
            int threshold = Integer.valueOf(params.getOrDefault(VALUE, "-1"));
            if (threshold == -1) {
                write2Client("no value = ? in SQL_PATTERN_THRESHOLD");
                return;
            }
            if (threshold >= 0 && threshold <= 1000) {
                EtracePatternUtil.setSqlPatternThreshold(threshold);
                logger.info("SQL_PATTERN_THRESHOLD is set to " + threshold);
                write2Client("SQL_PATTERN_THRESHOLD is set to " + threshold);
                return;
            }
            write2Client("fail to set SQL_PATTERN_THRESHOLD(hint: value range 0 ~ 1000)");
        } catch (Exception e) {
            logger.error("", e);
            write2Client("fail to set SQL_PATTERN_THRESHOLD");
        }
    }

    private void doShardedTables(Map<String, String> params) {
        try {
            String tableName = params.getOrDefault(TABLE_NAME, "");
            // 用户没有写表名参数
            if ("".equals(tableName)) {
                write2Client(
                    String.format("please input where %s = '<%s>'", TABLE_NAME, TABLE_NAME));
                return;
            }
            ShardingRouter router = sqlSessionContext.getShardingRouter();
            // 判断该表是否是sharding表
            if (router.isShardingTable(tableName)) {
                List<String> tables = new ArrayList<>();
                router.getAllShardingTable(tableName).forEach(t -> tables.add(t.table));
                write2Client(DEFAULT_RESULTSET_TITLE, tables);
                return;
            } else {
                write2Client(tableName + " is not sharded table,please check");
                return;
            }
        } catch (Exception e) {
            logger.error("", e);
        }
        write2Client(
            "fail to get all sharded tables, please input select help from dal_dual to see more details");
    }

    private void validateShardInfoParams(Map<String, String> params, ShardingRouter router) {
        String tableName = params.getOrDefault(TABLE_NAME, "");
        if (tableName.isEmpty()) {
            throw new IllegalArgumentException("no table_name in where clause");
        }
        if (!router.isShardingTable(tableName)) {
            throw new IllegalArgumentException(tableName + " is not sharding table");
        }
    }

    private void doShardInfo(Map<String, String> params) {
        try {
            ShardingRouter router = sqlSessionContext.getShardingRouter();
            //校验参数的合法性
            validateShardInfoParams(params, router);
            String tableName = params.remove(TABLE_NAME);

            String column = "";
            List<String> values = new ArrayList<>();
            //multi mapping column
            if (params.size() > 1) {
                List<String> columnList =
                    router.getMappingColumnsByColumnList(new ArrayList<>(params.keySet()));
                column = String.join(",", columnList);
                columnList.forEach(oneColumn -> {
                    values.add(params.get(oneColumn));
                });
            } else {
                Map.Entry<String, String> columnWithValue = params.entrySet().iterator().next();
                column = columnWithValue.getKey();
                values.add(columnWithValue.getValue());
            }
            handleShardInfo(tableName, column, String.join(",", values));
        } catch (Exception e) {
            logger.error("dal_dual get shard info error", e);
            write2Client("shard info",
                "fail to get shard info, please input select help from dal_dual to see more details");
        }
    }

    private void handleShardInfo(String tableName, String column, String value)
        throws QuitException {
        ShardingRouter router = sqlSessionContext.getShardingRouter();
        ComposedKey composedKey = router.getComposedKey(tableName);

        //column is composed column, eg: order id
        if (composedKey.getColumns().contains(column) && !"multi_mapping"
            .equals(composedKey.getSeq_name())) {
            String defaultColumn = composedKey.getShardingColumns().get(0);
            ShardingTable shardingTable =
                router.getShardingTableByComposedKey(tableName, defaultColumn, value);
            sendShardInfo(shardingTable);
            return;
        }
        // eg: user_id, restaurant_id, retailer_id | multi mapping key, eg:platform_tracking_id,platform_id
        if (composedKey.getExtractors().containsKey(column)) {
            ShardingTable shardingTable = router.getShardingTable(tableName, column, value);
            sendShardInfo(shardingTable);
            return;
        }
        // 经过了参数合法性校验，预期运行到此处必然后mapping_key配置
        MappingKey mappingKey =
            router.getMappingKey(tableName).entrySet().iterator().next().getValue();
        //column is mapping column, eg: platform_tracking_id
        handleShardInfoByMappingKey(router, mappingKey, tableName, column, value);
    }

    private void handleShardInfoByMappingKey(ShardingRouter router, MappingKey mappingKey,
        String tableName, String column, String value) throws QuitException {
        if (Objects.isNull(mappingKey)) {
            throw new IllegalArgumentException("unknown column: " + column);
        }
        if (mappingKey.containsSeqName(column)) {
            sendShardInfoByMappingKey(router, mappingKey, tableName, column, value);
        } else {
            throw new IllegalArgumentException("unknown column: " + column);
        }
    }

    private void sendShardInfoByMappingKey(ShardingRouter router, MappingKey mappingKey,
        String tableName, String column, String value) throws QuitException {
        ShardingTable mt = router.getMappingTableByMappingKey(tableName, mappingKey, column, value);
        DBConnectionInfo info = queryDBInfoByShardingTable(mt);
        String selectColumn = mappingKey.getColumn();
        String template = "SELECT %s FROM %s WHERE %s = '%s'";
        String query =
            String.format(template, selectColumn, mt.table, column.replaceAll(",", ""), value);
        AsyncResultSetHandler handler = (rs, isDone, reason) -> {
            if (!isDone) {
                logger.error("query mapping value failure, reason: " + reason);
                write2Client("shard info", reason);
                return;
            }
            if (rs.getResultType() == ResultType.ERR) {
                ERR err = rs.getErr();
                logger.error("get err response({}|{}) when query mapping value", err.errorCode,
                    err.errorMessage);
                write2Client("shard info", err.errorMessage);
                return;
            }
            if (!rs.next()) {
                String error =
                    String.format("no %s record found in mapping table %s", selectColumn, mt.table);
                logger.error(error);
                write2Client("shard info", error);
                return;
            }
            String result = rs.getString(1);
            ShardingTable t = router.getShardingTable(tableName, selectColumn, result);
            sendShardInfo(t);
        };
        AsyncOneTimeClient oneTimeClient = new AsyncOneTimeClient(info, query, handler);
        oneTimeClient.doAsyncExecute();
    }

    private DBConnectionInfo queryDBInfoByShardingTable(ShardingTable shardingTable)
        throws QuitException {
        DBChannelDispatcher dispatcher = sqlSessionContext.getHolder();
        String dbGroupName = shardingTable.database;
        String dbId = dispatcher.getDbGroup(dbGroupName).getDBServer(DBRole.SLAVE);
        return dispatcher.getSched(dbId).getInfo();
    }

    private void sendShardInfo(ShardingTable table) {
        if (Objects.isNull(table)) {
            write2Client("shard info",
                "fail to get shard info, please input select help from dal_dual to see more details");
            return;
        }
        try {
            DBConnectionInfo info = queryDBInfoByShardingTable(table);
            List<String> msgs = new LinkedList<>();
            msgs.add(String.format("%-17s | %s", "ip", info.getHost()));
            msgs.add(String.format("%-17s | %d", "port", info.getPort()));
            msgs.add(String.format("%-17s | %s", "database", info.getDatabase()));
            msgs.add(String.format("%-17s | %s", "sharded table", table.table));
            msgs.add(String.format("%-17s | %s", "qualified dbId", info.getQualifiedDbId()));
            write2Client("shard info", msgs);
        } catch (QuitException e) {
            logger.error("query db info failure", e);
            write2Client("shard info", e.getMessage());
        }
    }

    private void doUnsafeLog(boolean isOn) {
        UnsafeLog.unsafeLogOn.set(isOn);
        String type = isOn ? UNSAFE_SQL_ON : UNSAFE_SQL_OFF;
        logger.warn(type + " finished");
        write2Client(type + " finished");
    }

    private void doAutoreadSwitch(boolean isOn) {
        AutoreadSwitch.toggleAutoreadSwitch(isOn);
        write2Client(
            (isOn ? AutoreadSwitch.AUTOREAD_ON : AutoreadSwitch.AUTOREAD_OFF) + " finished");
    }

    private void write2Client(String msg) {
        write2Client(DEFAULT_RESULTSET_TITLE, msg);
    }

    private void write2Client(String title, String msg) {
        write2Client(title, Collections.singletonList(msg));
    }

    protected void write2Client(String title, List<String> msgs) {
        ResultSet rs = new ResultSet();
        rs.addColumn(new Column(title));
        for (String msg : msgs) {
            rs.addRow(new Row(msg));
        }
        sqlSessionContext
            .clientWriteAndFlush(Unpooled.wrappedBuffer(rs.toPackets().toArray(new byte[0][])));
    }

    private boolean handleShardingCount(SeqsParse seqsParse) throws SeqsException {
        if (!seqsParse.seqName.equals(GeneratorUtil.SHARDING_COUNT)) {
            return false;
        }
        String globalId = Integer.toString(getShardingCount(seqsParse.params.get(TABLE_NAME)));
        if (AthenaConfig.getInstance().getGrayType() == GrayType.NOSHARDING) {
            globalId = "-1";
        } else if (AthenaConfig.getInstance().getGrayType() == GrayType.SHARDING) {
        } else {// AthenaConfig.getInstance().getGrayType() == GrayType.GRAY
            if (!sqlSessionContext.grayUp.isGrayReadSession()) {
                globalId = "-1";
            }
        }
        List<String> globalIds = new ArrayList<>();
        globalIds.add(globalId);
        sqlSessionContext.clientWriteAndFlush(Unpooled.wrappedBuffer(
            GeneratorUtil.getIDPackets(GeneratorUtil.SHARDING_COUNT, globalIds)
                .toArray(new byte[0][])));
        return true;
    }

    private void doFakeGlobalIDSQL(SeqsParse seqsParse) throws QuitException {
        long beginTime = System.currentTimeMillis();
        try {
            if (handleShardingCount(seqsParse)) {
                MetricFactory.newTimer(Metrics.TIME_GLOBALID_DURATION).addTag(TraceNames.DALGROUP,
                    MetricMetaExtractor.extractDALGroupName(sqlSessionContext))
                    .addTag(TraceNames.DB_ROLE, "SLAVE").addTag(TraceNames.SQL_TYPE, "SELECT")
                    .setUpperEnable(true).value(System.currentTimeMillis() - beginTime);
                SQLLog.log(sqlSessionContext.curCmdQuery, sqlSessionContext.sessInfo(),
                    sqlSessionContext.getClientInfo(), null, System.currentTimeMillis() - beginTime,
                    -1, null, null);
                sqlSessionContext.sqlSessionContextUtil.endEtrace();
                sqlSessionContext.curCmdQuery = null;
                return;
            }
            // 设置status,不让客户端重新发送sql
            sqlSessionContext.setState(SESSION_STATUS.QUERY_RESULT);
            SeqsHandler seqsHandler =
                new SeqsHandler(1, seqsParse.seqName, sqlSessionContext.getHolder().getCfg(),
                    sqlSessionContext.sqlSessionContextUtil.getDalEtraceProducer(),
                    seqsParse.params, seqsParse.isRange, seqsParse.isSerial) {

                    @Override
                    public void clientWriteGlobalID(byte[][] globalIDData, boolean success,
                        String errMsg) {
                        if (!success) {
                            sqlSessionContext.quitTracer
                                .reportQuit(SessionQuitTracer.QuitTrace.GlobalIdErr,
                                    new ResponseStatus(ResponseStatus.ResponseType.ABORT,
                                        ErrorCode.ABORT_GLOBAL_ID,
                                        "failed to get globalid by server: " + errMsg));
                            sqlSessionContext.kill(ErrorCode.ABORT_GLOBAL_ID,
                                "failed to get globalid by server: " + errMsg, 1);
                            MetricFactory
                                .newCounterWithSqlSessionContext(Metrics.GLOBALID_SERVER_BROKEN,
                                    sqlSessionContext).once();
                            return;
                        }
                        try {
                            SQLLog.log(sqlSessionContext.curCmdQuery, sqlSessionContext.sessInfo(),
                                sqlSessionContext.getClientInfo(), null,
                                System.currentTimeMillis() - beginTime, -1, null, null);
                            sqlSessionContext.curCmdQuery = null;
                            sqlSessionContext.setState(SESSION_STATUS.QUERY_ANALYZE);
                            sqlSessionContext
                                .clientWriteAndFlush(Unpooled.wrappedBuffer(globalIDData));
                            sqlSessionContext.sqlSessionContextUtil.endEtrace();
                            MetricFactory
                                .newCounterWithSqlSessionContext(Metrics.TRANS_GLOBALID_QPS,
                                    sqlSessionContext).once();
                        } catch (Exception e) {
                            logger.error(
                                "Failed to get globalid by client " + sqlSessionContext.toString(),
                                e);
                            sqlSessionContext.quitTracer
                                .reportQuit(SessionQuitTracer.QuitTrace.GlobalIdErr);
                            sqlSessionContext
                                .kill(ErrorCode.ABORT_GLOBAL_ID, "failed to get globalid by client",
                                    1);
                            MetricFactory
                                .newCounterWithSqlSessionContext(Metrics.GLOBALID_CLIENT_BROKEN,
                                    sqlSessionContext).once();
                        } finally {
                            MetricFactory.newTimer(Metrics.TIME_GLOBALID_DURATION)
                                .addTag(TraceNames.DALGROUP,
                                    MetricMetaExtractor.extractDALGroupName(sqlSessionContext))
                                .addTag(TraceNames.DB_ROLE, "SLAVE")
                                .addTag(TraceNames.SQL_TYPE, "SELECT").setUpperEnable(true)
                                .value(System.currentTimeMillis() - beginTime);
                        }
                    }
                };
            seqsHandler.getGlobalID();
        } catch (SeqsException e) {
            logger.error(Objects.toString(e));
            sqlSessionContext.sqlSessionContextUtil.trySetResponseStatus(
                new ResponseStatus(ResponseStatus.ResponseType.DALERR,
                    ErrorCode.ERR_GLOBALID_SYNTAX_ERROR,
                    "seqs exception message: " + e.getMessage()));
            sqlSessionContext.sqlSessionContextUtil.endEtrace();
            SQLLog.log(sqlSessionContext.curCmdQuery, sqlSessionContext.sessInfo(),
                sqlSessionContext.getClientInfo(), null, System.currentTimeMillis() - beginTime, -1,
                null, null);
            sqlSessionContext.curCmdQuery = null;
            MetricFactory
                .newCounterWithSqlSessionContext(Metrics.GLOBALID_CLIENT_BROKEN, sqlSessionContext)
                .once();
            sqlSessionContext.setState(SESSION_STATUS.QUERY_ANALYZE);
            sqlSessionContext.clientWriteAndFlush(
                Unpooled.wrappedBuffer(GeneratorUtil.getErrPacket(e.getMessage())));
        } catch (Exception e) {
            logger.error(Objects.toString(e));
            MetricFactory
                .newCounterWithSqlSessionContext(Metrics.GLOBALID_SERVER_BROKEN, sqlSessionContext)
                .once();
            throw new QuitException(e.getMessage(), e);
        }
    }

    // 使得globalid和普通sql的duration的mtrics格式相同
    private static final Map<String, String> GLOBALID_MAP_PLACEHOLDER =
        new HashMap<String, String>() {
            private static final long serialVersionUID = 1L;

            {
                put("sql", "SELECT");
                put("role", "SLAVE");
            }
        };

    public int getShardingCount(String tableName) throws SeqsException {
        if (tableName == null) {
            throw new SeqsException(
                String.format("cannot find '%s' = ? in glbalid sql", TABLE_NAME));
        }
        if (sqlSessionContext.getShardingRouter().isShardingTable(tableName)) {
            return sqlSessionContext.getShardingRouter().getAllShardingTable(tableName).size();
        } else {
            return -1;
        }
    }

    private boolean tryExecuteControlFakeSql(ShardingSQL shardingSQL) {
        try {
            if (FAKE_SQLS.containsKey(shardingSQL.selected_value)) {
                FAKE_SQLS.get(shardingSQL.selected_value).doCtrlFakeSql(this, shardingSQL.params);
                return true;
            }
        } catch (Exception e) {
            logger.info("Exception when try execute control fake sql", e);
        }
        return false;
    }

    private void doQueryDbConfig(Map<String, String> params) {
        String dalGroup = params.get("group");
        List<DbConfigInfo> configInfos = DbConfigInfo.getDbConfigInfos(dalGroup);
        List<String> jsonConfigInfos = configInfos.stream().map(
            configInfo -> SafeJSONHelper.of(JacksonObjectMappers.getMapper())
                .writeValueAsStringOrDefault(configInfo, "")).filter(StringUtils::isNotEmpty)
            .collect(Collectors.toList());
        write2Client(DB_CONFIG, jsonConfigInfos);
    }

    private Optional<DBChannelDispatcher> queryDispatcher(Map<String, String> params) {
        if (params.containsKey(DAL_GROUP_NAME)) {
            return Optional
                .ofNullable(DBChannelDispatcher.getHolders().get(params.get(DAL_GROUP_NAME)));
        }
        return Optional.ofNullable(sqlSessionContext.getHolder());
    }

    /*
     * 配合BDI抽数，增加select tables from dal_dual;
     * 跟show tables的区别为，增加shard的逻辑分表，去掉shard实际分表
     * 没查到任何时，返回empty(参考show tables)
     * */
    private void doQueryShowTables() {
        try {
            DBChannelDispatcher dispatcher = sqlSessionContext.getHolder();
            String dbId = dispatcher.getHomeDbGroup().getDBServer(DBRole.SLAVE);
            DBConnectionInfo info = dispatcher.getSched(dbId).getInfo();
            ShardingRouter router = this.sqlSessionContext.getShardingRouter();
            //逻辑shard table
            Set<String> originShardingTables = router.getAllOriginShardedTables();
            //实际shard table
            List<String> authenticShardingTables = new ArrayList<>();
            List<String> tableNames = new LinkedList<>();
            AsyncResultSetHandler handler = (rs, isDone, reason) -> {
                if (!isDone) {
                    logger.warn("select tables failure, reason : ", reason);
                    write2Client(TABLES, Collections.emptyList());
                    return;
                }
                originShardingTables.forEach(st -> router.getAllShardingTable(st)
                    .forEach(t -> authenticShardingTables.add(t.table)));
                while (rs.next()) {
                    String tableName = rs.getString(1);
                    if (originShardingTables.contains(tableName)) {
                        originShardingTables.remove(tableName);
                    }
                    tableNames.add(tableName);
                }
                tableNames.addAll(originShardingTables);
                tableNames.removeAll(authenticShardingTables);
                write2Client(TABLES, tableNames);
            };
            AsyncOneTimeClient oneTimeClient = new AsyncOneTimeClient(info, "show tables", handler);
            oneTimeClient.doAsyncExecute();
        } catch (Exception e) {
            logger.warn("select tables failure , errmsg : ", e);
            write2Client(TABLES, Collections.emptyList());
        }

    }
}
