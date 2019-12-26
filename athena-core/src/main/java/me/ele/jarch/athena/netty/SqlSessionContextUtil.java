package me.ele.jarch.athena.netty;

import com.github.mpjct.jmpjct.util.ErrorCode;
import io.etrace.common.Constants;
import io.etrace.common.modal.Transaction;
import me.ele.jarch.athena.SQLLog;
import me.ele.jarch.athena.allinone.DBRole;
import me.ele.jarch.athena.allinone.Footprint;
import me.ele.jarch.athena.constant.EventTypes;
import me.ele.jarch.athena.constant.Metrics;
import me.ele.jarch.athena.constant.TraceNames;
import me.ele.jarch.athena.detector.DetectorDelegate;
import me.ele.jarch.athena.netty.state.FakeSqlState;
import me.ele.jarch.athena.netty.state.SESSION_STATUS;
import me.ele.jarch.athena.scheduler.AutoKiller;
import me.ele.jarch.athena.scheduler.DBChannelDispatcher;
import me.ele.jarch.athena.server.pool.ServerSession;
import me.ele.jarch.athena.sharding.sql.IterableShardingResult;
import me.ele.jarch.athena.sharding.sql.SQLPatternAnalyzer;
import me.ele.jarch.athena.sharding.sql.ShardingSQL;
import me.ele.jarch.athena.sql.CmdQuery;
import me.ele.jarch.athena.sql.QUERY_TYPE;
import me.ele.jarch.athena.sql.seqs.SeqsParse;
import me.ele.jarch.athena.util.*;
import me.ele.jarch.athena.util.ResponseStatus.ResponseType;
import me.ele.jarch.athena.util.etrace.DalMultiMessageProducer;
import me.ele.jarch.athena.util.etrace.MetricFactory;
import me.ele.jarch.athena.util.etrace.MetricMetaExtractor;
import me.ele.jarch.athena.util.log.RawSQLContxt;
import me.ele.jarch.athena.util.log.RawSQLFilterTask;
import me.ele.jarch.athena.util.rmq.AuditSqlToRmq;
import me.ele.jarch.athena.util.rmq.RmqSqlInfo;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Pattern;

import static me.ele.jarch.athena.constant.Constants.ATHENA_ADMIN_READONLY;

public class SqlSessionContextUtil {
    private static final Logger logger = LoggerFactory.getLogger(SqlSessionContextUtil.class);
    public volatile long cRecentRecvTime = 0;
    private QueryStatistics queryStatistics = QueryStatistics.EMPTY;
    private QueryStatistics shardingQueryStatistics = QueryStatistics.EMPTY;
    private QueryStatistics grayQueryStatistics = QueryStatistics.EMPTY;
    private volatile TransStatistics transStatistics = TransStatistics.EMPTY;
    private DalMultiMessageProducer dalEtraceProducer =
        DalMultiMessageProducer.createEmptyProducer();
    private Transaction loginTransaction = null;
    private DalMultiMessageProducer SqlTransactionProducer =
        DalMultiMessageProducer.createEmptyProducer();
    private long proxyDuration = 0;
    private long serverDuration = 0;
    public DBRole dbRole = DBRole.MASTER;

    // 用于记录第二个commit失败的情况,如果该值是偶数,表示正常,奇数表示第二个commit未返回
    public volatile long secondCommitId = 0;

    public static final Pattern TABLE_P = Pattern
        .compile("FROM\\s+(\\w+).*JOIN\\s+(\\w+)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    private volatile ResponseStatus responseStatus =
        new ResponseStatus(ResponseStatus.ResponseType.OK);
    private volatile long lastSQLTimeInMills = 0L;
    private final Map<String, Long> loginPhrases =
        Collections.synchronizedMap(new LinkedHashMap<>());

    public boolean trySetResponseStatus(ResponseStatus newResponseStatus) {
        //如果之前已经设置过响应状态为DBERR或DALERR. 再次尝试设置响应状态为ABORT将设置失败
        if ((this.responseStatus.getResponseType() == ResponseType.DBERR
            || this.responseStatus.getResponseType() == ResponseType.DALERR)
            && newResponseStatus.getResponseType() == ResponseType.ABORT) {
            return false;
        }
        //如果当前响应状态不是OK,并且尝试设置一个非OK状态,将无法设置成功。
        if (this.responseStatus.getResponseType() != ResponseType.OK
            && newResponseStatus.getResponseType() != ResponseType.OK) {
            return false;
        }
        if (newResponseStatus.getResponseType() == ResponseType.DBERR) {
            MetricFactory.newCounterWithSqlSessionContext(Metrics.SQL_DBERROR, sqlSessionContext)
                .once();
        }
        if (newResponseStatus.getResponseType() == ResponseType.DALERR) {
            MetricFactory.newCounterWithSqlSessionContext(Metrics.SQL_DALERROR, sqlSessionContext)
                .once();
        }
        if (newResponseStatus.getResponseType() == ResponseType.ABORT) {
            MetricFactory.newCounterWithSqlSessionContext(Metrics.SQL_ABORT, sqlSessionContext)
                .once();
        }
        if (newResponseStatus.getResponseType() == ResponseType.CROSS_EZONE) {
            MetricFactory
                .newCounterWithSqlSessionContext(Metrics.SQL_CROSS_EZONE, sqlSessionContext).once();
        }

        this.responseStatus = newResponseStatus;
        return true;
    }

    private SqlSessionContext sqlSessionContext;

    public SqlSessionContextUtil(SqlSessionContext sqlSessionContext) {
        this.sqlSessionContext = sqlSessionContext;
    }

    public String getPacketTime() {
        return queryStatistics.toString();
    }

    public String getTransTime() {
        return transStatistics.getTransTime();
    }

    public void doStatistics() {
        if (responseStatus.getResponseType() == ResponseType.OK
            || responseStatus.getResponseType() == ResponseType.DBERR) {
            sendMetricBySQLType(sqlSessionContext.getQueryType());
        }
        if (AutoKiller.isSchedulerInAutoKill(sqlSessionContext.scheduler)) {
            MetricFactory.newCounterWithSqlSessionContext(Metrics.AK_PASSED, sqlSessionContext)
                .once();
        }
        if (responseStatus.getResponseType() == ResponseType.OK) {
            QPSCounter.getInstance().incrQPS(sqlSessionContext.scheduler);
        }

        if (this.sqlSessionContext.isInShardedMode) {
            long proxyDur = shardingQueryStatistics.getProxyDur();
            long serverDur = shardingQueryStatistics
                .getServerDur();// The duration for receiving all package of response
            MetricFactory.newTimerWithSqlSessionContext(Metrics.DURATION_PROXY, sqlSessionContext)
                .addTag(TraceNames.DB_ROLE, dbRole.name()).setUpperEnable(true).value(proxyDur);
            if (responseStatus.getResponseType() != ResponseType.DALERR) {
                MetricFactory
                    .newTimerWithSqlSessionContext(Metrics.DURATION_SERVER, sqlSessionContext)
                    .addTag(TraceNames.DB_ROLE, dbRole.name()).setUpperEnable(true)
                    .value(serverDur);
            }
            doShardingSQLLog(proxyDur, serverDur);
            this.shardingQueryStatistics = QueryStatistics.EMPTY;
        } else {
            long proxyDur = queryStatistics.getProxyDur();
            long serverDur =
                queryStatistics.getServerDur();// The duration for receiving all package of response
            MetricFactory.newTimerWithSqlSessionContext(Metrics.DURATION_PROXY, sqlSessionContext)
                .addTag(TraceNames.DB_ROLE, dbRole.name()).setUpperEnable(true).value(proxyDur);
            if (responseStatus.getResponseType() != ResponseType.DALERR) {
                MetricFactory
                    .newTimerWithSqlSessionContext(Metrics.DURATION_SERVER, sqlSessionContext)
                    .addTag(TraceNames.DB_ROLE, dbRole.name()).setUpperEnable(true)
                    .value(serverDur);
            }
            MetricFactory.newTimer(Metrics.DURATION_CPROXY).addTag(TraceNames.DALGROUP,
                MetricMetaExtractor.extractDALGroupName(sqlSessionContext))
                .addTag(TraceNames.DB_ROLE, dbRole.name())
                .addTag(TraceNames.SQL_TYPE, sqlSessionContext.getQueryType().name())
                .setUpperEnable(true).value(proxyDur);
            doSQLLog(proxyDur, serverDur);
            this.proxyDuration = proxyDur;
            this.serverDuration = serverDur;
            recordSQLPatternDuration(serverDur);
            DetectorDelegate.addSample(serverDur, sqlSessionContext);
            this.resetQueryTimeRecord();
            tryRecordSQLAffected();
        }
    }

    private void recordSQLPatternDuration(long duration) {
        if (responseStatus.getResponseType() == ResponseStatus.ResponseType.DALERR) {
            return;
        }
        if (Objects.nonNull(sqlSessionContext.curCmdQuery) && Objects
            .nonNull(sqlSessionContext.curCmdQuery.shardingSql)) {
            SQLPatternAnalyzer.getInst()
                .produceSQLPattern(sqlSessionContext.curCmdQuery.shardingSql.getMostSafeSQL(),
                    duration);
        }
    }

    public void doGrayStatistics() {
        grayQueryStatistics.setcSendTime(System.currentTimeMillis());
        long proxyDur = grayQueryStatistics.getProxyDur();
        long serverDur = grayQueryStatistics.getServerDur();
        MetricFactory.newTimerWithSqlSessionContext(Metrics.DURATION_GRAY_PROXY, sqlSessionContext)
            .value(proxyDur);
        MetricFactory.newTimerWithSqlSessionContext(Metrics.DURATION_GRAY_SERVER, sqlSessionContext)
            .value(serverDur);
        if (sqlSessionContext.curCmdQuery != null && sqlSessionContext.curCmdQuery.query != null) {
            SQLLog.log(sqlSessionContext.curCmdQuery, sqlSessionContext.grayUp.getSessionInfo(),
                sqlSessionContext.getClientInfo(), responseStatus, proxyDur, serverDur,
                grayQueryStatistics.getProxyTime(), "GRAY");
        }
        this.proxyDuration = proxyDur;
        this.serverDuration = serverDur;
        grayQueryStatistics = QueryStatistics.EMPTY;

    }

    public void doShardingSQLLog(long proxyDur, long serverDur) {
        String shardedTable = null;
        if (sqlSessionContext.curCmdQuery != null && sqlSessionContext.curCmdQuery.query != null) {
            if (sqlSessionContext.isInShardedMode
                && sqlSessionContext.shardingResult.currentResult != null) {
                shardedTable = sqlSessionContext.shardingResult.currentResult.shardTable;
            }
            if (sqlSessionContext.curCmdQuery.isSharded()) {
                // log all sharded tables once
                IterableShardingResult r = sqlSessionContext.shardingResult;
                if (r.currentResult != null && !r.tables.isEmpty() && r.currentResult.shardTable
                    .equals(r.tables.get(0))) {
                    SQLLog.logWithFilter(sqlSessionContext.transactionId,
                        sqlSessionContext.curCmdQuery, null, sqlSessionContext.getClientInfo(),
                        responseStatus, 0, 0, null, null);
                }
            }
            SQLLog.logWithFilter(sqlSessionContext.transactionId, sqlSessionContext.curCmdQuery,
                sqlSessionContext.currentSessInfo(), sqlSessionContext.getClientInfo(),
                responseStatus, proxyDur, serverDur, shardingQueryStatistics.getProxyTime(),
                shardedTable);
        }
    }

    /**
     * Do the log for original sql
     *
     * @param proxyDur
     * @param serverDur
     */
    public void doSQLLog(long proxyDur, long serverDur) {
        if (sqlSessionContext.curCmdQuery != null && sqlSessionContext.curCmdQuery.query != null) {
            SQLLog.logWithFilter(sqlSessionContext.transactionId, sqlSessionContext.curCmdQuery,
                sqlSessionContext.sessInfo(), sqlSessionContext.getClientInfo(), responseStatus,
                proxyDur, serverDur, queryStatistics.getProxyTime(), null);
        }
    }

    public void resetQueryTimeRecord() {
        this.queryStatistics = QueryStatistics.EMPTY;
        this.lastSQLTimeInMills = System.currentTimeMillis();
    }

    public QueryStatistics startShardingQueryStatistics(CmdQuery query) {
        if (query == null) {
            logger.error("error to startShardingQueryStatistics, query is null");
            this.shardingQueryStatistics = QueryStatistics.EMPTY;
        } else {
            this.queryStatistics.setShardedMode(true);
            this.shardingQueryStatistics = new QueryStatistics();
            this.shardingQueryStatistics.setcRecvTime(System.currentTimeMillis());
            this.queryStatistics.appendShardingQueryStatistics(this.shardingQueryStatistics);
        }
        return queryStatistics;
    }

    public void endAllShardingQueryStatistics() {
        this.queryStatistics.setcSendTime(System.currentTimeMillis());
        //如果在执行映射sql过程中提前返回,记录Metrics
        if (sqlSessionContext.isInMappingShardedMode) {
            MetricFactory
                .newCounterWithSqlSessionContext(Metrics.MAPPING_SQL_FAIL, sqlSessionContext)
                .once();
        }
        MetricFactory.newTimer(Metrics.DURATION_CPROXY)
            .addTag(TraceNames.DALGROUP, MetricMetaExtractor.extractDALGroupName(sqlSessionContext))
            .addTag(TraceNames.DB_ROLE, dbRole.name())
            .addTag(TraceNames.SQL_TYPE, sqlSessionContext.getQueryType().name())
            .setUpperEnable(true).value(queryStatistics.getProxyDur());
        if (sqlSessionContext.curCmdQuery != null && sqlSessionContext.curCmdQuery.query != null) {
            SQLLog.logWithFilter(sqlSessionContext.transactionId, sqlSessionContext.curCmdQuery,
                sqlSessionContext.sessInfo(), sqlSessionContext.getClientInfo(), responseStatus,
                this.queryStatistics.getProxyDur(), this.queryStatistics.getServerDur(),
                queryStatistics.getProxyTime(), "Origin");
            DetectorDelegate.addSample(this.queryStatistics.getServerDur(), sqlSessionContext);
            recordSQLPatternDuration(this.queryStatistics.getServerDur());
            this.proxyDuration = queryStatistics.getProxyDur();
            this.serverDuration = queryStatistics.getServerDur();
        }
        this.queryStatistics = QueryStatistics.EMPTY;
        tryRecordSQLAffected();
    }

    private void tryRecordSQLAffected() {
        long affectedRows = getAffectedRows();
        if (affectedRows < 0) {
            // 表明SQL没有执行成功
            return;
        }
        QUERY_TYPE curQueryType = sqlSessionContext.getCurQueryType();
        if (Objects.equals(QUERY_TYPE.DELETE, curQueryType) || Objects
            .equals(QUERY_TYPE.UPDATE, curQueryType)) {
            //@formatter:off
            MetricFactory.newTimer(Metrics.SQL_AFFECTED).setUpperEnable(true)
                    .addTag(TraceNames.DALGROUP, sqlSessionContext.getHolder().getDalGroup().getName())
                    .addTag(TraceNames.SQL_TYPE, curQueryType.name())
                    .addTag(TraceNames.SQL_ID, sqlSessionContext.curCmdQuery.getSqlId())
                    .value(affectedRows);
            //@formatter:on
        }
    }

    public QueryStatistics startQueryStatistics(CmdQuery query) {
        if (query == null) {
            logger.error("error to startQueryStatistics query is null");
            this.queryStatistics = QueryStatistics.EMPTY;
        } else {
            this.queryStatistics = new QueryStatistics();
            this.queryStatistics.setcRecvTime(cRecentRecvTime);
            if (sqlSessionContext.isInTransStatus()
                && this.transStatistics.getTransId() == sqlSessionContext.transactionId) {
                this.transStatistics.appendQueryStatistics(queryStatistics);
            }
        }
        return queryStatistics;
    }

    /**
     * 开启在dal中etrace的继续跟踪
     *
     * @param query
     */
    public Transaction startEtrace(CmdQuery query) {
        if (query == null) {
            return null;
        }
        // 新建producer并且持续跟踪
        this.dalEtraceProducer = DalMultiMessageProducer.createProducer();
        return this.dalEtraceProducer.startTransactionAndGet("DAL", "");
    }

    /**
     * 继续dal中etrace的跟踪
     *
     * @param trans 开始的事务
     * @param query 查询实例
     */
    public void continueEtrace(Transaction trans, CmdQuery query) {
        if (trans == null || query == null) {
            return;
        }
        trans.setName(getOpWithTableName(query));
        this.dalEtraceProducer.continueTrace(query.rid, query.rpcid);
        String group =
            sqlSessionContext.getHolder() == null ? "" : sqlSessionContext.getHolder().getCfg();
        this.dalEtraceProducer.addTag("group", group);
        this.dalEtraceProducer.addTag("clientIp", getClientIpPort());
        this.dalEtraceProducer.addTag("request",
            String.valueOf(query.getSendBuf() != null ? query.getSendBuf().length : "-1"));
        this.dalEtraceProducer.addTag("clientAppId", query.appid);
        this.dalEtraceProducer
            .addTag("clientConnId", String.valueOf(sqlSessionContext.connectionId));
        this.dalEtraceProducer.addTag("userName", sqlSessionContext.authenticator.userName);
        this.dalEtraceProducer.addTag("transId", String.valueOf(sqlSessionContext.transactionId));
        // globalid需要另加
        this.dalEtraceProducer.addTag("shardingkey", query.shardKeyAndValue);
        if (Objects.nonNull(query.shardingSql) && StringUtils
            .isNotEmpty(query.shardingSql.getOriginWhiteFields())) {
            dalEtraceProducer
                .addTag(TraceNames.WHITE_FIELDS, query.shardingSql.getOriginWhiteFields());
        }
        // 当非globalid sql时,才添加pattern
        // 当获取globalid时,不在此处添加pattern,在startGlobalIdEtrace()处添加
        if (FakeSqlState.isGetGlobalIdSQL(query)) {
            return;
        }
        //当sql上DALGROUP自检sql时，统一将sqlpattern改成dal_health_check_query
        if (ATHENA_ADMIN_READONLY.equals(sqlSessionContext.authenticator.userName)) {
            this.dalEtraceProducer.addPattern2CurTransaction("dal_health_check_query",
                "4916d2e32086ec4433fe31d7892d95e4");
            return;
        }
        this.dalEtraceProducer.addPattern2CurTransaction(query.getSqlPattern(), query.getSqlId());
    }

    /**
     * 结束etrace跟踪
     */
    public void endEtrace() {
        //如果是dal heartbeat用户,则不记录response tag
        if (!sqlSessionContext.authenticator.isDalHealthCheckUser) {
            trySendResponseSizeMetric();
            String response = String.valueOf(sqlSessionContext.writeToClientCounts == 0L ?
                -1 :
                sqlSessionContext.writeToClientCounts);
            String sqlWaitTime = String.valueOf(this.proxyDuration > this.serverDuration ?
                this.proxyDuration - this.serverDuration :
                0);
            String sqlExecTime = String.valueOf(this.serverDuration > 0 ? this.serverDuration : 0);
            this.dalEtraceProducer.addTag("response", response);
            this.dalEtraceProducer.addTag("sqlWaitTime", sqlWaitTime);
            this.dalEtraceProducer.addTag("sqlExecTime", sqlExecTime);
            // 向auditSqlInfo中添加tag信息
            NoThrow.call(
                () -> RmqSqlInfo.addTags(sqlSessionContext, response, sqlWaitTime, sqlExecTime));
        }
        // 尝试发送rmqSqlInfo
        NoThrow.call(() -> AuditSqlToRmq.getInstance().trySend(sqlSessionContext));
        sqlSessionContext.writeToClientCounts = 0L;
        appendRespStatus();
        // 如果出现有些事务没有设置状态，说明程序有漏提交事务的地方，正常情况下，调用此方法时事务栈中只有一个事务
        this.dalEtraceProducer.completeAllTransaction();
        this.dalEtraceProducer = DalMultiMessageProducer.createEmptyProducer();
    }

    private void trySendResponseSizeMetric() {
        if (sqlSessionContext.writeToClientCounts < GreySwitch.getInstance()
            .getMaxResponseLength()) {
            return;
        }
        String dalGroup = Objects.isNull(sqlSessionContext.getHolder()) ?
            "EMPTY" :
            sqlSessionContext.getHolder().getDalGroup().getName();
        this.dalEtraceProducer.newFluentEvent(EventTypes.RISK_BIG_RESULT, dalGroup)
            .data("grade:" + lengthGrading(sqlSessionContext.writeToClientCounts))
            .status(Constants.SUCCESS).complete();
    }

    /**
     * 当sqlCtx关闭退出时,尝试提交所有未提交的Etrace事务
     * 注意: 该方法只是作为防御性代码,理论上不应该被调用
     * 如果被调用则说明前面的状态机发生了未预期的事,导致
     * 有Etrace事务未提交
     */
    void endEtraceWhenQuit() {
        String transactionStatus = String.format("%s:%s", ResponseType.DALERR, ErrorCode.ERR);
        dalEtraceProducer.addTag("errorMsg", "complete when do quit");
        dalEtraceProducer.setTransactionStatus(transactionStatus);
        this.dalEtraceProducer.completeAllTransaction();
    }

    public void startLoginEtrace() {
        this.dalEtraceProducer = DalMultiMessageProducer.createProducer();
        this.loginTransaction =
            this.dalEtraceProducer.startTransactionAndGet("DAL", "unknown.login");
        this.dalEtraceProducer.addTag("clientIp", getClientIpPort());
    }

    public void continueLoginEtrace(String username, DBChannelDispatcher holder) {
        this.dalEtraceProducer
            .addTag("clientConnId", String.valueOf(sqlSessionContext.connectionId));
        if (ATHENA_ADMIN_READONLY.equals(username)) {
            this.dalEtraceProducer.addPattern2CurTransaction("dal_health_check_login");
        } else {
            this.dalEtraceProducer.addPattern2CurTransaction("login");
        }
        Objects.requireNonNull(username);
        if (Objects.nonNull(this.loginTransaction)) {
            this.loginTransaction.setName(String.format("%s.login", username));
        }
        //由于登录时没有发送到dal的sql,故此处数据量为-1
        this.dalEtraceProducer.addTag("request", String.valueOf("-1"));
        String mainGroup = Objects.nonNull(holder) ? holder.getCfg() : getPotentialDalGroupName();
        this.dalEtraceProducer.addTag("group", mainGroup);
        this.dalEtraceProducer.addTag("bind2master", String.valueOf(sqlSessionContext.bind2master));
        this.loginTransaction = null;
    }

    public SqlSessionContextUtil appendLoginTimeLineEtrace() {
        this.dalEtraceProducer.addTag("timeline", printLoginTimeLine());
        return this;
    }

    public SqlSessionContextUtil appendFootprintLineEtrace(Footprint footprint) {
        this.dalEtraceProducer.addTag("footprint", footprint.name());
        return this;
    }

    private SqlSessionContextUtil appendIndexHintsLineEtraceIfExit() {
        String curIndexHints = sqlSessionContext.curCmdQuery.getIndexHints();
        if (!curIndexHints.isEmpty()) {
            this.dalEtraceProducer.addTag("indexHints", curIndexHints);
        }
        return this;
    }

    public SqlSessionContextUtil appendEZoneShardInfoEtrace(String shardIds) {
        this.dalEtraceProducer.addTag("injectShardRewriteInfo", shardIds);
        return this;
    }

    public SqlSessionContextUtil appendEZoneShardInfoEvent(String conditions) {
        String dalGroup = Objects.isNull(sqlSessionContext.getHolder()) ?
            "EMPTY" :
            sqlSessionContext.getHolder().getDalGroup().getName();
        this.dalEtraceProducer.newFluentEvent(EventTypes.BINE_EZONE_REWRITE, dalGroup)
            .data("conditions:" + conditions).status(Constants.SUCCESS).complete();
        return this;
    }

    public void sendLoginErrMetric(String name, String errorMsg) {
        logger.error(errorMsg, sqlSessionContext.fixedSessInfo(), sqlSessionContext.getClientInfo(),
            printLoginTimeLine());
        MetricFactory.newCounter(name)
            .addTag(TraceNames.DALGROUP, MetricMetaExtractor.extractDALGroupName(sqlSessionContext))
            .once();
    }

    public void sendSuspectPingMetric(String name, String errorMsg) {
        logger.warn(errorMsg, sqlSessionContext.fixedSessInfo(), sqlSessionContext.getClientInfo(),
            printLoginTimeLine());
        MetricFactory.newCounter(name)
            .addTag(TraceNames.DALGROUP, MetricMetaExtractor.extractDALGroupName(sqlSessionContext))
            .once();
    }

    public void continueHealthCheckEtrace(String userName) {
        Objects.requireNonNull(userName);
        if (Objects.nonNull(this.loginTransaction)) {
            this.loginTransaction.setType("HEARTBEAT");
            this.loginTransaction.setName(String.format("%s", userName));
        }
        this.dalEtraceProducer.addTag("hostName", me.ele.jarch.athena.constant.Constants.HOSTNAME);
        this.loginTransaction = null;

    }

    public void startGlobalIdEtrace() {
        CmdQuery curCmdQuery = sqlSessionContext.curCmdQuery;
        this.dalEtraceProducer.clean();
        this.dalEtraceProducer = DalMultiMessageProducer.createProducer();
        this.dalEtraceProducer.startTransactionAndGet("GLOBALID", "dal_dual.select");
        this.dalEtraceProducer.continueTrace(curCmdQuery.rid, curCmdQuery.rpcid);
        this.dalEtraceProducer.addTag("group", sqlSessionContext.getHolder().getCfg());
        this.dalEtraceProducer.addTag("clientIp", getClientIpPort());
        this.dalEtraceProducer.addTag("clientAppId", curCmdQuery.appid);
        this.dalEtraceProducer
            .addTag("clientConnId", String.valueOf(sqlSessionContext.connectionId));
        this.dalEtraceProducer.addTag("transId", String.valueOf(sqlSessionContext.transactionId));
        this.dalEtraceProducer.addTag("shardingkey", curCmdQuery.shardKeyAndValue);
        this.dalEtraceProducer
            .addPattern2CurTransaction(curCmdQuery.getSqlPattern(), curCmdQuery.getSqlId());
        SeqsParse seqsParse = sqlSessionContext.seqsParse;
        this.dalEtraceProducer
            .addTag("seqsName", seqsParse.params.getOrDefault("biz", seqsParse.seqName));

        //add sub SEQ transaction for dal_dual sql
        this.dalEtraceProducer.startTransaction("SEQ", "dal_dual.select");
        this.dalEtraceProducer.addTag("connId", String.valueOf(sqlSessionContext.connectionId));
        this.dalEtraceProducer
            .addPattern2CurTransaction(curCmdQuery.getSqlPattern(), curCmdQuery.getSqlId());
        this.dalEtraceProducer.setTransactionStatus(Constants.SUCCESS);
        this.dalEtraceProducer.completeTransaction();
    }

    /**
     * 用于在etraceProducer的事务栈中新添加一个DB事务
     * 操作的类型提取自当前正在执行的sql,发送的字节数从当前的SendBuf中提取。
     *
     * @param ss 使用的db serversession
     */
    public void startNewTransaction(ServerSession ss) {
        CmdQuery query = this.sqlSessionContext.curCmdQuery;
        if (ss == null || query == null) {
            return;
        }
        String opName = getOpWithTableName(query);
        int requestBytesSize = Objects.isNull(query.getSendBuf()) ? -1 : query.getSendBuf().length;
        startNewTransaction(ss, opName, requestBytesSize);
    }

    /**
     * 用于在etraceProducer的事务栈中新添加一个DB事务,此api提供了更大的灵活性，可以由调用方指定
     * 操作类型,以及发往db的字节数。
     *
     * @param ss               使用的db serversession
     * @param opName           sql的表名.操作类型
     * @param requestBytesSize 发往db的字节数
     */
    public void startNewTransaction(ServerSession ss, String opName, int requestBytesSize) {
        this.dalEtraceProducer.startTransaction(Constants.SQL, opName);
        this.dalEtraceProducer.addTag("dbId", ss.getActiveQulifiedDbId());
        this.dalEtraceProducer.addTag("connId", String.valueOf(ss.getConnectionId()));
        this.dalEtraceProducer.addTag("role", String.valueOf(ss.getOriginDbConnInfo().getRole()));
        this.dalEtraceProducer.addTag("dbInfo", ss.dbFullName);
        this.dalEtraceProducer
            .addTag("request", String.valueOf(requestBytesSize < 0 ? -1 : requestBytesSize));
        //在该DB事务创建时，在Etrace上新增 SQL路由选择 的tag
        this.appendFootprintLineEtrace(this.sqlSessionContext.curFootprint);
        this.appendIndexHintsLineEtraceIfExit();
    }

    public void appendAwaitDbConnTime(long awaitTimeMilli) {
        this.dalEtraceProducer.addTag("acquireDBConnTime", String.valueOf(awaitTimeMilli));
    }

    /**
     * 用于提交在etraceProducer的事务栈中栈顶的事务
     */
    public void completeTransaction() {
        this.dalEtraceProducer.addTag("response", String.valueOf(
            this.sqlSessionContext.currentWriteToDalCounts == 0L ?
                -1 :
                this.sqlSessionContext.currentWriteToDalCounts));
        this.sqlSessionContext.currentWriteToDalCounts = 0L;
        this.dalEtraceProducer.addTag("affectedRows", String.valueOf(getAffectedRows()));
        appendRespStatus();
        this.dalEtraceProducer.completeTransaction();
    }

    /**
     * 开始追踪每条映射sql的执行
     */
    public void startMappingETrace() {
        CmdQuery curCmdQuery = sqlSessionContext.curCmdQuery;
        startInnerEtrace(curCmdQuery.shardingSql.getMostSafeSQL(),
            Optional.ofNullable(curCmdQuery.shardingSql.getWhiteFields()));
    }

    /**
     * 结束对单条映射sql Etrace跟踪
     */
    public void endMappingEtrace() {
        if (responseStatus.getResponseType() != ResponseType.OK) {
            String transactionStatus = String.format("%s:%s", responseStatus.getResponseType(),
                responseStatus.getCodeOrDefaultDALERR().getProtoName());
            dalEtraceProducer.addTag("errorMsg", responseStatus.getMessage());
            endInnerEtrace(transactionStatus);
            return;
        }
        endInnerEtrace(Constants.SUCCESS);
    }

    /**
     * 开启Etrace DAL事务内部嵌套的事务
     *
     * @param sqlPattern
     */
    public void startInnerEtrace(String sqlPattern, Optional<String> whiteFieldsOp) {
        CmdQuery curCmdQuery = sqlSessionContext.curCmdQuery;
        dalEtraceProducer.startTransactionAndGet("DAL", getOpWithTableName(curCmdQuery));
        dalEtraceProducer.continueTrace(curCmdQuery.rid, curCmdQuery.rpcid);
        dalEtraceProducer.addTag("group", sqlSessionContext.getHolder().getCfg());
        dalEtraceProducer.addTag("clientIp", getClientIpPort());
        dalEtraceProducer.addTag("request", String.valueOf(curCmdQuery.getSendBuf().length));
        dalEtraceProducer.addTag("response", String.valueOf(-1));
        dalEtraceProducer.addTag("clientAppId", curCmdQuery.appid);
        dalEtraceProducer.addTag("clientConnId", String.valueOf(sqlSessionContext.connectionId));
        dalEtraceProducer.addTag("transId", String.valueOf(sqlSessionContext.transactionId));
        whiteFieldsOp.ifPresent(
            whiteFields -> dalEtraceProducer.addTag(TraceNames.WHITE_FIELDS, whiteFields));
        dalEtraceProducer.addPattern2CurTransaction(sqlPattern);
    }

    public void startInnerEtrace(String sqlPattern) {
        startInnerEtrace(sqlPattern, Optional.empty());
    }

    /**
     * 结束Etrace DAL事务内部嵌套的事务
     */
    public void endInnerEtrace(String transactionStatus) {
        dalEtraceProducer.setTransactionStatus(transactionStatus);
        dalEtraceProducer.completeTransaction();
    }

    /**
     * 为了串起未带注释的sql事务而伪造Producer
     *
     * @return
     */
    public DalMultiMessageProducer startSqlTransactionProducerAndGet() {
        SqlTransactionProducer = DalMultiMessageProducer.createProducer();
        SqlTransactionProducer.startTransaction("TRANS", "FAKE");
        return SqlTransactionProducer;
    }

    public DalMultiMessageProducer getSqlTransactionProducer() {
        return SqlTransactionProducer;
    }

    /**
     * 结束伪造的Producer,并设置结束状态
     *
     * @param transactionStatus
     */
    public void endSqlTransactionProducer(String transactionStatus) {
        SqlTransactionProducer.setTransactionStatus(transactionStatus);
        SqlTransactionProducer.completeTransaction();
        SqlTransactionProducer = DalMultiMessageProducer.createEmptyProducer();
    }

    /**
     * 获取sql的type，格式：tableName.queryType
     *
     * @param query
     * @return
     */
    private String getOpWithTableName(CmdQuery query) {
        if (query == null) {
            return "";
        }
        String tableName = StringUtils.isEmpty(query.table) ? "unkowntable" : query.table;
        String opWithTable =
            new StringBuilder(64).append(tableName).append('.').append(query.curQueryType)
                .toString();
        return opWithTable.toLowerCase();
    }

    /**
     * 追加事务的响应状态信息到当前DalMultiMessageProducer
     */
    private void appendRespStatus() {
        if (responseStatus.getResponseType() != ResponseType.OK) {
            String transactionStatus = String.format("%s:%s", responseStatus.getResponseType(),
                responseStatus.getCodeOrDefaultDALERR().getProtoName());
            dalEtraceProducer.addTag("errorMsg", responseStatus.getMessage());
            dalEtraceProducer.setTransactionStatus(transactionStatus);
        } else {
            dalEtraceProducer.setTransactionStatus(Constants.SUCCESS);
        }
    }

    /**
     * 获取连接dal的客户端的ip地址
     *
     * @return
     */
    private String getClientIpPort() {
        String clientAddr = sqlSessionContext.getClientAddr();
        clientAddr = StringUtils.isEmpty(clientAddr) ? "" : clientAddr;
        int index = clientAddr.indexOf("/");
        String clientIp = index < 0 ? clientAddr : clientAddr.substring(index + 1);
        return clientIp;
    }

    public long getAffectedRows() {
        if (sqlSessionContext != null && sqlSessionContext.curCmdQuery != null) {
            return sqlSessionContext.curCmdQuery.affectedRows;
        }
        return -1;
    }

    /**
     * SqlCtx无DBChannelDispatcher时根据Client传入的schema搜寻匹配的dalgroup name
     *
     * @return dal group name 或"unknown"
     */
    public String getPotentialDalGroupName() {
        String schema = sqlSessionContext.authenticator.getSchema();
        return DBChannelDispatcher.getHolders().keySet().contains(schema) ? schema : "unknown";
    }

    public TransStatistics startTransStatistics(String transId, String rid) {
        this.transStatistics = new TransStatistics(transId, rid);
        this.transStatistics.settStartTime(System.currentTimeMillis());
        this.transStatistics.appendQueryStatistics(queryStatistics);
        return transStatistics;
    }

    public QueryStatistics startGrayQueryStatistics(CmdQuery query) {
        if (query == null) {
            logger.error("error to startGrayQueryStatistics query is null");
            this.grayQueryStatistics = QueryStatistics.EMPTY;
        } else {
            this.grayQueryStatistics = new QueryStatistics();
            this.grayQueryStatistics.setcRecvTime(this.cRecentRecvTime);
        }
        return queryStatistics;
    }

    public void resetTransTimeRecord() {
        this.transStatistics = TransStatistics.EMPTY;
    }

    /**
     * @return normal QueryStatistics or the sub sharding QueryStatistics
     */
    public QueryStatistics getCurrentQueryStatistics() {
        if (this.sqlSessionContext.getStatus() == SESSION_STATUS.GRAY_RESULT) {
            return grayQueryStatistics;
        }
        return this.queryStatistics.getCurrentQueryStatistics();
    }

    public QueryStatistics getQueryStatistics() {
        return this.queryStatistics;
    }

    public TransStatistics getTransStatistics() {
        return transStatistics;
    }

    public QueryStatistics getGrayQueryStatistics() {
        return grayQueryStatistics;
    }

    public void sendClientQPSOrTPSMetrics(QUERY_TYPE queryType) {
        if (queryType == QUERY_TYPE.SELECT) {
            MetricFactory.newCounter(Metrics.TRANS_CQPS).addTag(TraceNames.DALGROUP,
                MetricMetaExtractor.extractDALGroupName(sqlSessionContext)).once();
        }
        if (queryType.isWritableStatement()) {
            MetricFactory.newCounter(Metrics.TRANS_CTPS).addTag(TraceNames.DALGROUP,
                MetricMetaExtractor.extractDALGroupName(sqlSessionContext))
                .addTag("sql", queryType.name()).once();
        }
    }

    /**
     * 检查客户端发来的SQL是否有风险
     * 目前只检查SQL是否是sharding扫全片的查询
     *
     * @param cmdQuery
     */
    public void sendMetricIfSQLWithRisk(CmdQuery cmdQuery) {
        String dalGroup = sqlSessionContext.getHolder().getDalGroup().getName();
        String sqlId = cmdQuery.getSqlId();
        if (cmdQuery.query.length() > GreySwitch.getInstance().getMaxSqlLength()) {
            String requestGrade =
                sqlSessionContext.sqlSessionContextUtil.lengthGrading(cmdQuery.query.length());
            this.dalEtraceProducer.newFluentEvent(EventTypes.RISK_SQL_TOO_LONG, dalGroup)
                .data("grade:" + requestGrade).status(Constants.SUCCESS).complete();
        }
        ShardingSQL shardingSQL = cmdQuery.shardingSql;
        if (Objects.isNull(shardingSQL)) {
            return;
        }
        if (shardingSQL.sqlFeature.isNeedSharding() && shardingSQL.sqlFeature
            .isShardingSelectAll()) {
            //@formatter:off
            MetricFactory.newCounter(Metrics.SQL_RISK_SHARDINGSELECTALL)
                    .addTag(TraceNames.SQL_ID, sqlId)
                    .addTag(TraceNames.TABLE, cmdQuery.originalTable)
                    .addTag(TraceNames.DALGROUP, dalGroup)
                    .once();
            this.dalEtraceProducer.newFluentEvent(EventTypes.RISK_SQL_SHARDING_SCAN_ALL, dalGroup)
                    .status(Constants.SUCCESS)
                    .complete();
            //@formatter:on
        }
    }

    public void tryFilterDangerSQL(CmdQuery cmdQuery) {
        if (!GreySwitch.getInstance().isDangerSqlFilterEnabled()) {
            return;
        }
        final String queryWithoutComment = cmdQuery.queryWithoutComment;
        // 原始SQL小于15字符或者大于128KB字符不再进行后续处理, 原因: 大于128KB的单条LOG, sls会丢弃
        if (queryWithoutComment.length() < 15 || queryWithoutComment.length() > 65536) {
            return;
        }
        RawSQLContxt container = new RawSQLContxt().clientInfo(sqlSessionContext.getClientInfo())
            .user(sqlSessionContext.authenticator.userName)
            .dalGroup(sqlSessionContext.getHolder().getDalGroup().getName())
            .transactionId(sqlSessionContext.transactionId).rawSQL(cmdQuery.query)
            .sqlWithoutComment(queryWithoutComment);
        AthenaServer.DANGER_SQL_FILTER_EXECUTOR.submit(new RawSQLFilterTask(container));
    }

    /**
     * 根据SQL类型，记录对应类型的Metric
     *
     * @param type
     */
    public void sendMetricBySQLType(QUERY_TYPE type) {
        if (type.isWritableStatement()) {
            MetricFactory.newCounterWithSqlSessionContext(Metrics.TRANS_TPS, sqlSessionContext)
                .once();
        } else if (type.isReadOnlyStatement()) {
            MetricFactory.newCounterWithSqlSessionContext(Metrics.TRANS_QPS, sqlSessionContext)
                .once();
        }
    }

    public ResponseStatus getResponseType() {
        return this.responseStatus;
    }

    public DalMultiMessageProducer getDalEtraceProducer() {
        return dalEtraceProducer;
    }

    public long getLastSQLTimeInMills() {
        return lastSQLTimeInMills;
    }

    public SqlSessionContextUtil appendLoginPhrase(String phraseName) {
        this.loginPhrases.put(phraseName, System.currentTimeMillis());
        return this;
    }

    private String printLoginTimeLine() {
        StringBuilder sb = new StringBuilder();
        this.loginPhrases.forEach((phrase, instant) -> {
            TimeFormatUtil.appendTime(phrase, sb, instant);
        });
        if (!this.loginPhrases.isEmpty()) {
            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
    }

    public long getProxyDuration() {
        return proxyDuration;
    }

    /**
     * 对长度进行分级
     * invalid:-1, [0,10K):1K, [10K, 100K):10K,
     * [100K,512K):100K, [512K,1M):512k, [1M,5M):1M, [5M,10M):5M >=10M:10M
     */
    public String lengthGrading(long length) {
        if (length < 0) {
            return "-1";
        } else if (length < 1_000) {
            return "0";
        } else if (length < 10_000) {
            return "1k";
        } else if (length < 100_000) {
            return "10k";
        } else if (length < 512_000) {
            return "100k";
        } else if (length < 1_000_000) {
            return "512k";
        } else if (length < 5_000_000) {
            return "1m";
        } else if (length < 10_000_000) {
            return "5m";
        } else if (length < 50_000_000) {
            return "10m";
        } else if (length < 100_000_000) {
            return "50m";
        } else {
            return "100m";
        }
    }
}
