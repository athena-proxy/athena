package me.ele.jarch.athena.netty.state;

import com.github.mpjct.jmpjct.mysql.proto.*;
import com.github.mpjct.jmpjct.util.ErrorCode;
import io.etrace.common.modal.Transaction;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import me.ele.jarch.athena.SQLLog;
import me.ele.jarch.athena.SQLLogFilter;
import me.ele.jarch.athena.allinone.DBGroup;
import me.ele.jarch.athena.allinone.DBRole;
import me.ele.jarch.athena.allinone.Footprint;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.constant.EventTypes;
import me.ele.jarch.athena.constant.Metrics;
import me.ele.jarch.athena.constant.TraceNames;
import me.ele.jarch.athena.exception.QueryException;
import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.netty.AthenaServer;
import me.ele.jarch.athena.netty.SessionQuitTracer;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.scheduler.HangSessionMonitor;
import me.ele.jarch.athena.scheduler.MulScheduler.PayloadType;
import me.ele.jarch.athena.scheduler.SQLTrafficPolicing;
import me.ele.jarch.athena.scheduler.Scheduler;
import me.ele.jarch.athena.server.pool.ServerSession;
import me.ele.jarch.athena.sharding.sql.*;
import me.ele.jarch.athena.sql.CmdQuery;
import me.ele.jarch.athena.sql.QUERY_TYPE;
import me.ele.jarch.athena.sql.QueryPacket;
import me.ele.jarch.athena.sql.rscache.ResultPacketsCache;
import me.ele.jarch.athena.sql.seqs.GeneratorUtil;
import me.ele.jarch.athena.sql.seqs.SeqsParse;
import me.ele.jarch.athena.util.*;
import me.ele.jarch.athena.util.etrace.DalMultiMessageProducer;
import me.ele.jarch.athena.util.etrace.MetricFactory;
import me.ele.jarch.athena.util.etrace.MetricMetaExtractor;
import me.ele.jarch.athena.worker.SchedulerWorker;
import me.ele.jarch.athena.worker.manager.DelayResponseManager;
import me.ele.jarch.athena.worker.manager.ManualKillerManager;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import static me.ele.jarch.athena.sharding.ShardingConfig.SHARDING_ID_DELIMITER;
import static me.ele.jarch.athena.sql.QUERY_TYPE.JDBC_PREPARE_STMT;

public class QueryAnalyzeState implements State {
    public static final Logger logger = LoggerFactory.getLogger(QueryAnalyzeState.class);
    protected static final AtomicLong commitIdGen = new AtomicLong();
    private static final AtomicLong transactionIdGen = new AtomicLong();

    protected SqlSessionContext sqlSessionContext;

    public QueryAnalyzeState(SqlSessionContext sqlSessionContext) {
        this.sqlSessionContext = sqlSessionContext;
    }

    @Override public boolean handle() throws QuitException {
        return doQueryAnalyze();
    }

    @Override public SESSION_STATUS getStatus() {
        return SESSION_STATUS.QUERY_ANALYZE;
    }

    protected CmdQuery newCmdQuery(byte[] packet, String dalGroup) {
        return new CmdQuery(packet, dalGroup);
    }

    protected QueryPacket newQueryPacket() {
        return new Com_Query();
    }

    protected boolean doQueryAnalyze() throws QuitException {
        CmdQuery curCmdQuery = null;
        initEverySql();

        if (sqlSessionContext.isInShardedMode) {
            sqlSessionContext.shardingResult.next();
            curCmdQuery = sqlSessionContext.curCmdQuery;
            sqlSessionContext.sqlSessionContextUtil.startShardingQueryStatistics(curCmdQuery);
            QueryPacket comQuery = newQueryPacket();
            ShardingResult currentResult = sqlSessionContext.shardingResult.currentResult;
            comQuery.setQuery(currentResult.shardedSQL);
            curCmdQuery.query = currentResult.shardedSQL;
            // 替换当前CmdQuery的table为sharding后sql的table
            curCmdQuery.table = currentResult.shardTable;
            //重写curQueryType 用于Etrace追踪
            curCmdQuery.curQueryType = currentResult.queryType;
            curCmdQuery.rewriteBytes(comQuery.toPacket());
        } else {
            sqlSessionContext.shardingResult = new SimpleIterableShardingResult();
            byte[] packet = sqlSessionContext.clientPackets.poll();
            if (packet == null) {
                throw new QuitException("clientPackets is null." + sqlSessionContext);
            }
            curCmdQuery =
                newCmdQuery(packet, sqlSessionContext.getHolder().getDalGroup().getName());
            if (sqlSessionContext.batchCond.isBatchAllowed()) {
                curCmdQuery.batchCond =
                    sqlSessionContext.cmdQueryBatchCond.copyValuesFrom(sqlSessionContext.batchCond);
            }
            sqlSessionContext.curCmdQuery = curCmdQuery;
            sqlSessionContext.sqlSessionContextUtil.startQueryStatistics(curCmdQuery);
            // 开启etrace跟踪
            Transaction trans = sqlSessionContext.sqlSessionContextUtil.startEtrace(curCmdQuery);
            try {
                extractQuery(curCmdQuery);
            } finally {
                computeAndSetTransId(curCmdQuery.queryType);
                newTraceInfo4SQLTransactionIfAbsent(curCmdQuery);
                sqlSessionContext.sqlSessionContextUtil.continueEtrace(trans, curCmdQuery);
                sqlSessionContext.sqlSessionContextUtil
                    .sendClientQPSOrTPSMetrics(curCmdQuery.queryType);
                sqlSessionContext.sqlSessionContextUtil.sendMetricIfSQLWithRisk(curCmdQuery);
                sqlSessionContext.sqlSessionContextUtil.tryFilterDangerSQL(curCmdQuery);
            }
            sqlSessionContext.resetNeedBatchAnalyzeAfterShard(curCmdQuery);
            //检查当前dispatcher是否已下线
            if (!validateDispatcher()) {
                return false;
            }
            // 只读用户查询检验
            if (!validateReadOnly(curCmdQuery)) {
                return false;
            }

            // 检验SQL是否要被Zk配置拒绝
            validateRejectSQL(curCmdQuery);

            // 检查SQL是否为JDBC Prepare statement,如果是,那么放行第一条sql,且拒绝第二条sql
            if (!validateJDBCPrepareStmt(curCmdQuery)) {
                return false;
            }

            // 此处处理可能追加index hints的情况
            rewriteIndexHintsIfConfigured(curCmdQuery);

            if (AthenaConfig.getInstance().getGrayType() == GrayType.GRAY) {
                if (curCmdQuery.isSharded() && !sqlSessionContext.isInTransStatus()
                    && curCmdQuery.queryType.isWritableStatement()) {
                    sqlSessionContext.grayUp.setStarted(true);
                    sqlSessionContext.grayUp.setInShardingTrans(true);
                } else if (!sqlSessionContext.isInTransStatus()) {
                    sqlSessionContext.grayUp.setInShardingTrans(false);
                }
                if (!sqlSessionContext.grayUp.isInShardingTrans() && sqlSessionContext
                    .isInTransStatus() && curCmdQuery.isSharded() && curCmdQuery.queryType
                    .isWritableStatement()) {
                    logger.error("Not allowed to use sharding query in non sharding transaction ! "
                        + this.sqlSessionContext.toString());
                }
                sqlSessionContext.grayUp.query = curCmdQuery;
                if (sqlSessionContext.grayUp.isGrayReadSession() && curCmdQuery.isSharded()
                    && curCmdQuery.queryType == QUERY_TYPE.SELECT) {
                    curCmdQuery.setGrayRead(true);
                }
            }

            if (AthenaConfig.getInstance().getGrayType() != GrayType.SHARDING) {
                if (curCmdQuery.shardingSql != null) {
                    curCmdQuery.shardingSql.needSharding = false;
                    curCmdQuery.shardingSql.resetShardingResult();
                }
            }

            if (curCmdQuery.isSharded()) {
                sqlSessionContext.isInShardedMode = true;
                if (curCmdQuery.hasMappingShardingSql()) {
                    sqlSessionContext.shardingResult = curCmdQuery.shardingSql.getMappingResults();
                    sqlSessionContext.isInMappingShardedMode = true;
                } else {
                    sqlSessionContext.initSharingContext(curCmdQuery.shardingSql.results);
                }
                if (sqlSessionContext.shardingResult.tables.size() > 1
                    && sqlSessionContext.shardingResult.groupByItems.size() > 0) {
                    logger
                        .warn("Not support group by multiple sharding. " + this.sqlSessionContext);
                }
                sqlSessionContext.setState(SESSION_STATUS.QUERY_ANALYZE);
                sqlSessionContext.getHolder().enqueue(sqlSessionContext);
                return false;
            } else if (sqlSessionContext.isInTransStatus() && (curCmdQuery.queryType.isEndTrans()
                || curCmdQuery.queryType.isForSavePoint()) && sqlSessionContext.shardingCount > 0) {
                sqlSessionContext.shardingResult = generateShardingResultForEndTransQuery(
                    this.sqlSessionContext.curCmdQuery.query);
                sqlSessionContext.shardingContext =
                    sqlSessionContext.newShardingContext(sqlSessionContext.quitTracer);
                sqlSessionContext.isInShardedMode = true;
                sqlSessionContext.shardingContext
                    .init(sqlSessionContext.shardingResult, curCmdQuery.originalTable,
                        curCmdQuery.queryType);
                sqlSessionContext.setState(SESSION_STATUS.QUERY_ANALYZE);
                sqlSessionContext.getHolder().enqueue(sqlSessionContext);
                return false;
            }
        }

        if (!sqlSessionContext.isInTransStatus() && sqlSessionContext.shardedSession.size() > 0) {
            throw new QuitException("strange! not in transaction but session is exsiting");
        }

        //如果输入的table是xxxdb.table
        if (GreySwitch.getInstance().isSpecifyDb() && StringUtils
            .isNotEmpty(curCmdQuery.specifyDB)) {
            sqlSessionContext.sqlSessionContextUtil.getDalEtraceProducer()
                .newFluentEvent(EventTypes.SPECIFY_DB,
                    sqlSessionContext.getHolder().getDalGroup().getName())
                .data("specify DB : " + curCmdQuery.specifyDB)
                .status(io.etrace.common.Constants.SUCCESS).complete();
        }

        boolean fakeCommit_Rollback =
            (!sqlSessionContext.isInTransStatus()) && sqlSessionContext.getQueryType().isEndTrans();

        if (curCmdQuery.queryType == QUERY_TYPE.KILL_CMD) {
            excuteKillCmd(curCmdQuery.shardingSql);
            return false;
        }

        if (handleEarlyReturnQuery(curCmdQuery, fakeCommit_Rollback)) {
            // 支持PG的pipeline式的请求
            if (sqlSessionContext.allowUnsyncQuery() && !sqlSessionContext.clientPackets
                .isEmpty()) {
                sqlSessionContext.getHolder().enqueue(sqlSessionContext);
            }
            return false;
        }

        if (!validateSavepointRelatedQuery()) {
            return false;
        }

        final SeqsParse seqsParse = new SeqsParse();
        // 当访问dal_dual的sql不符合dal_dual语法,如缺少where条件等情况
        // 那么直接报错,而不是发往db
        if (GeneratorUtil.DAL_DUAL.equalsIgnoreCase(curCmdQuery.table)) {
            if (seqsParse.parseGlobalIDSQL(curCmdQuery)) {
                seqsParse.checkSeqParamCount();
                sqlSessionContext.seqsParse = seqsParse;
                sqlSessionContext.setState(SESSION_STATUS.FAKE_SQL);
                doDispatcher();
                return false;
            } else if (FakeSqlState.isFakeSQL(curCmdQuery)) {
                sqlSessionContext.setState(SESSION_STATUS.FAKE_SQL);
                doDispatcher();
                return false;
            } else {
                throw new QueryException.Builder(ErrorCode.ER_SYNTAX_ERROR).setSequenceId(1)
                    .setErrorMessage(
                        "dal_dual sql syntax error,please input [select help from dal_dual]")
                    .bulid();
            }
        } else {
            sqlSessionContext.setState(SESSION_STATUS.QUERY_HANDLE);
        }

        // 执行mapping sql 时不校验shardingId
        if (!sqlSessionContext.isInMappingShardedMode) {
            validateShardingId();
        }
        if (curCmdQuery.isGrayRead()) {
            String id = sqlSessionContext.getHolder().getHomeDbGroup().getDBServer(DBRole.GRAY);
            sqlSessionContext.scheduler = sqlSessionContext.getHolder().getSched(id);
            sqlSessionContext.setState(SESSION_STATUS.GRAY_QUERY_HANDLE);
            sqlSessionContext.getHolder().enqueue(sqlSessionContext);
            sqlSessionContext.sqlSessionContextUtil
                .startGrayQueryStatistics(sqlSessionContext.curCmdQuery);
            return false;
        }
        doScheduler(curCmdQuery.queryType);
        // 如果处于执行映射sql的过程中,开启对映射sql的追踪
        if (sqlSessionContext.isInMappingShardedMode) {
            sqlSessionContext.sqlSessionContextUtil.startMappingETrace();
        }
        return false;
    }

    private boolean handleEarlyReturnQuery(CmdQuery curCmdQuery, boolean fakeCommit_Rollback) {
        Optional<ByteBuf> fakeResponseOp =
            tryFakeEarlyReturnQueryResponse(curCmdQuery, fakeCommit_Rollback);
        if (!fakeResponseOp.isPresent()) {
            return false;
        }
        // 由于fake commit 和 rollback的量很大,因此需要记录下数据量
        sqlSessionContext.writeToClientCounts = fakeResponseOp.get().readableBytes();
        preEndTrace();
        sqlSessionContext.clientWriteAndFlush(fakeResponseOp.get());
        return true;
    }

    protected Optional<ByteBuf> tryFakeEarlyReturnQueryResponse(CmdQuery curCmdQuery,
        boolean fakeCommit_Rollback) {
        ByteBuf fakeResponse = null;
        // 处理需要伪造具体ResultSet结果集的SQL
        if (curCmdQuery.queryType.isFakeResultStatement()) {
            switch (curCmdQuery.queryType) {
                case SHOW_WARNINGS:
                    fakeResponse = Unpooled.wrappedBuffer(ShardingOtherSQL.SHOW_WARNINGS_PACKET);
                    break;
                case QUERY_AUTOCOMMIT:
                    fakeResponse = handleQueryAutocommit(curCmdQuery);
                    break;
                case MYSQL_PING:
                    fakeResponse = Unpooled.wrappedBuffer(sqlSessionContext.buildOk());
                    break;
                default:
                    fakeResponse = Unpooled.wrappedBuffer(sqlSessionContext.buildOk());
                    break;
            }
            // 处理set autocommit,除了返回OK以外,还要进行一些设置
        } else if (curCmdQuery.queryType == QUERY_TYPE.SET_AUTOCOMMIT) {
            executeSetAutoCommitCmd(curCmdQuery);
            fakeResponse = sqlSessionContext.buildOk();
            // 处理普通SET,USE等
        } else if (curCmdQuery.queryType == QUERY_TYPE.BEGIN) {
            fakeResponse = handleBegin();
        } else if (curCmdQuery.queryType == QUERY_TYPE.IGNORE_CMD) {
            fakeResponse = handleIgnoreCmd();
        } else if (fakeCommit_Rollback) {
            // 处理不在事务中得commit和rollback
            fakeResponse = handleFakeCommitRollback(curCmdQuery);
        }
        return Optional.ofNullable(fakeResponse);
    }

    protected ByteBuf handleBegin() {
        //TODO 事务不支持嵌套，是否需要处理事务中执行START TRANSACTION的情况
        sqlSessionContext.transactionController.markImplicitTransaction();
        return sqlSessionContext.buildOk();
    }

    private ByteBuf handleQueryAutocommit(CmdQuery curCmdQuery) {
        ShardingSQL shardingSQL = curCmdQuery.shardingSql;
        ResultSet rs = new ResultSet();
        rs.addColumn(new Column(shardingSQL.getQueryAutoCommitHeader()));
        String autocommitValue;
        if (shardingSQL.isQueryGlobalAutoCommit()) {
            autocommitValue = "1";
        } else {
            autocommitValue = sqlSessionContext.transactionController.isAutoCommit() ? "1" : "0";
        }
        rs.addRow(new Row(autocommitValue));
        ByteBuf buf = Unpooled.buffer();
        for (byte[] arr : rs.toPackets()) {
            buf.writeBytes(arr);
        }
        return buf;
    }

    // MYSQL的实现为返回OK包,PG需复写该方法
    protected ByteBuf handleFakeCommitRollback(CmdQuery curCmdQuery) {
        // 处理START TRANSACTION -> COMMIT场景
        sqlSessionContext.transactionController.onTransactionEnd();
        return sqlSessionContext.buildOk();
    }

    // MYSQL的实现为返回OK包,PG需复写该方法
    protected ByteBuf handleIgnoreCmd() {
        return sqlSessionContext.buildOk();
    }

    protected void initEverySql() {
        sqlSessionContext.grayUp.isReadySend = false;
        sqlSessionContext.sqlSessionContextUtil
            .trySetResponseStatus(new ResponseStatus(ResponseStatus.ResponseType.OK));
    }

    protected void doDispatcher() {
        sqlSessionContext.getHolder().enqueue(sqlSessionContext);
    }

    private void computeAndSetTransId(QUERY_TYPE queryType) {
        if (noNeedNewTransId()) {
            return;
        }
        if (needNonTransPrefix(queryType)) {
            sqlSessionContext.transactionId =
                Constants.QUERY_PREFIX + transactionIdGen.incrementAndGet();
            return;
        }
        if (isAboutStartNewTrans(queryType)) {
            sqlSessionContext.transactionId =
                Constants.TRANS_PREFIX + transactionIdGen.incrementAndGet();
            return;
        }
        if (needAutoCommitPrefix(queryType)) {
            sqlSessionContext.transactionId =
                Constants.AUTOCOMMIT_PREFIX + transactionIdGen.incrementAndGet();
            return;
        }
        logger.warn(
            "strange when compute transId, QUERY_TYPE: {}, transaction status: {}, transId: {}",
            queryType, sqlSessionContext.isInTransStatus(), sqlSessionContext.transactionId);
    }

    private void newTraceInfo4SQLTransactionIfAbsent(CmdQuery cmdQuery) {
        // to simple, we only check rid
        if (Objects.nonNull(cmdQuery.rid)) {
            return;
        }
        if (sqlSessionContext.isInTransStatus()) {
            DalMultiMessageProducer producer =
                sqlSessionContext.sqlSessionContextUtil.getSqlTransactionProducer();
            if (producer.currentRequestId().isEmpty()) {
                // in transacton, previous sql has rid, current sql no rid
                // use "DAL" MessageProducer generated rid
                return;
            }
            cmdQuery.rid = producer.currentRequestId();
            cmdQuery.rpcid = producer.nextRemoteRpcId();
            return;
        }
        // about to start new transaction
        if (isAboutStartNewTrans(cmdQuery.queryType)) {
            DalMultiMessageProducer producer =
                sqlSessionContext.sqlSessionContextUtil.startSqlTransactionProducerAndGet();
            cmdQuery.rid = producer.currentRequestId();
            cmdQuery.rpcid = producer.nextRemoteRpcId();
            return;
        }
        // single query without transaction use default rid
    }

    private boolean noNeedNewTransId() {
        return sqlSessionContext.isInTransStatus() || StringUtils.isNotEmpty(
            sqlSessionContext.batchCond.getValueFromComment(Constants.ELE_META_TRANSID));
    }

    protected boolean isIllegalSavePointRealatedQuery() {
        return !sqlSessionContext.isInTransStatus() && sqlSessionContext.getQueryType()
            .isForSavePoint();
    }

    protected boolean needNonTransPrefix(QUERY_TYPE queryType) {
        return !queryType.isWritableStatement() && !isOpenTransactionStatment(queryType);
    }

    protected boolean isAboutStartNewTrans(QUERY_TYPE queryType) {
        return (queryType.isWritableStatement() && sqlSessionContext.transactionController
            .canOpenTransaction()) || isOpenTransactionStatment(queryType);
    }

    protected boolean needAutoCommitPrefix(QUERY_TYPE queryType) {
        return queryType.isWritableStatement() && !sqlSessionContext.transactionController
            .canOpenTransaction();
    }

    protected boolean preferNonAutoCommit() {
        return sqlSessionContext.transactionController.canOpenTransaction()
            && !sqlSessionContext.isInMappingShardedMode;
    }

    protected boolean isOpenTransactionStatment(QUERY_TYPE queryType) {
        return queryType == QUERY_TYPE.BEGIN || queryType == QUERY_TYPE.SET_AUTOCOMMIT
            || queryType == QUERY_TYPE.COMMIT;
    }

    protected void doScheduler(QUERY_TYPE queryType) throws QuitException {
        boolean isBenginTrx = false;
        String dbGroupName = null;
        if (sqlSessionContext.isInShardedMode) {
            dbGroupName = sqlSessionContext.shardingResult.currentResult.shardDB;
        }
        String dbId = null;
        if (!sqlSessionContext.isInTransStatus()) {
            if (queryType.isWritableStatement()) {
                // 是否是开启事务语句, 该方法在PG时只有BEGIN才会返回true
                // 在MYSQL时,还要判断是否是autocommit,是否是START TRANSACTION
                if (preferNonAutoCommit()) {
                    isBenginTrx = true;
                    sqlSessionContext.commitId = commitIdGen.incrementAndGet();
                    sqlSessionContext.setTransStatus(true);
                }
                sqlSessionContext.shardingId = sqlSessionContext.isInShardedMode ?
                    sqlSessionContext.shardingResult.currentResult.id :
                    "";
                dbId = getDBServerID(dbGroupName, DBRole.MASTER);
                sqlSessionContext.sqlSessionContextUtil.dbRole = DBRole.MASTER;
                sqlSessionContext.setTransDbid(dbId);
            } else if (isForceQueryMaster()) {
                dbId = getDBServerID(dbGroupName, DBRole.MASTER);
                sqlSessionContext.sqlSessionContextUtil.dbRole = DBRole.MASTER;
            } else if (queryType.isReadOnlyStatement()) {
                dbId = getDBServerID(dbGroupName, DBRole.SLAVE);
                sqlSessionContext.sqlSessionContextUtil.dbRole = DBRole.SLAVE;
            } else if (queryType.isUnknownTrans()) {
                byte type =
                    sqlSessionContext.curCmdQuery != null ? sqlSessionContext.curCmdQuery.type : -1;
                if (isLogSQLType(type)) {
                    SQLLogFilter.error(
                        "Other SQL Type: " + sqlSessionContext.curCmdQuery.getTypeName() + " "
                            + type + " " + sqlSessionContext.getCurQuery() + " "
                            + sqlSessionContext.sqlSessionContextUtil.getPacketTime());
                }
                dbId = getDBServerID(dbGroupName, DBRole.SLAVE);
                sqlSessionContext.sqlSessionContextUtil.dbRole = DBRole.SLAVE;
            } else if (queryType == QUERY_TYPE.TRUNCATE) {
                dbId = getDBServerID(dbGroupName, DBRole.MASTER);
                sqlSessionContext.sqlSessionContextUtil.dbRole = DBRole.MASTER;
            }
        } else if (sqlSessionContext.shardedSession.size() == 0) {
            throw new QuitException("strange! in transaction but session is null");
        } else {
            // choose the current dbId for getSched()
            if (dbGroupName == null) {
                // for non sharding transaction
                if (this.sqlSessionContext.shardingId.isEmpty()) {
                    ServerSession session =
                        sqlSessionContext.shardedSession.entrySet().iterator().next().getValue();
                    dbId = session.getActiveQulifiedDbId();
                } else {// for sharding transaction, choose a default slave for this query
                    dbId = getDBServerID(dbGroupName, DBRole.SLAVE);
                }
            } else {
                ServerSession session = sqlSessionContext.shardedSession.get(dbGroupName);
                if (session != null) {
                    dbId = session.getActiveQulifiedDbId();
                } else {
                    dbId = getDBServerID(dbGroupName, DBRole.MASTER);
                }
            }
            if (queryType.isEndTrans()) {
                sqlSessionContext.commitId = -1;
            }
        }

        if (dbId == null) {
            throw new QuitException("strange! dbId is null");
        }
        Scheduler scheduler = sqlSessionContext.getHolder().getSched(dbId);
        if (queryCachedResponse(scheduler)) {
            return;
        }
        sqlSessionContext.scheduler = scheduler;
        sqlSessionContext.curFootprint = getSQLFootprintType(queryType);
        sqlSessionContext.sqlSessionContextUtil
            .appendFootprintLineEtrace(sqlSessionContext.curFootprint);
        // 如果是commit/rollback,则跳过Scheduler调度层(信号量限制),直接进入SchdulerWorker队列执行。
        // 否则继续之前的逻辑
        if (queryType.isEndTrans()) {
            sqlSessionContext.getHolder().enqueue(sqlSessionContext);
            return;
        }
        // 经batch分析过的sql,则跳过Scheduler调度层(信号量限制),直接进入SchdulerWorker队列执行。
        if (sqlSessionContext.batchCond.isShuffledByBatchContext()) {
            sqlSessionContext.getHolder().enqueue(sqlSessionContext);
            return;
        }
        PayloadType type = isBenginTrx ? PayloadType.TRX : PayloadType.QUERY;
        SQLTrafficPolicing tp = sqlSessionContext.getHolder()
            .getSqlTrafficPolicing(sqlSessionContext.curCmdQuery.getSqlId());
        if (tp != null) {
            final Scheduler sche = sqlSessionContext.scheduler;
            final boolean final_isBenginTrx = isBenginTrx;
            tp.doPolicing(() -> sche.enqueue(sqlSessionContext, type, final_isBenginTrx));
        } else {
            sqlSessionContext.scheduler.enqueue(sqlSessionContext, type, isBenginTrx);
        }
    }

    /**
     * 获取SQL 在DAL中的路由选择类型
     * 当 bind2master 为TRUE的时候，返回 master_port
     * 当 bind2master 为FALSE，且SQL后的Where条件有 'bind_master'='bind_master' 返回 bind_master
     * 当 bind2master 为FALSE，且SQL后没有'bind_master'='bind_master'时：
     * - 如果 dbInfo里面的 Role为SLAVE，则认为是 正常的读写分离，返回 auto_port
     * - 如果 dbInfo里面的 Role为MASTER，且ZK配置是 bind_master，返回 auto_port
     * - 如果 dbInfo里面的 Role为MASTER，且SQL是ZK配置的`manual_bind_master_sqls`, 返回 MANUAL_REDIRECT2MASTER
     * - 如果 dbInfo里面的 Role为MASTER，且在事务中，返回 auto_port
     * - 如果 dbInfo里面的 Role为MASTER，且不在事务中：
     * - queryType是 只读的，返回：redirect2master（无有效SLAVE）
     * - queryType是 可写的，返回：auto_port（正常的逻辑）
     */
    private Footprint getSQLFootprintType(QUERY_TYPE queryType) {
        if (sqlSessionContext.bind2master) {
            return Footprint.MASTER_PORT;
        }
        if (sqlSessionContext.curCmdQuery.isBindMasterExpr) {
            return Footprint.BIND_MASTER;
        }
        DBRole dbRole = sqlSessionContext.scheduler.getInfo().getRole();
        if (dbRole == DBRole.SLAVE) {
            return Footprint.AUTO_PORT;
        }
        if (dbRole != DBRole.MASTER) {
            return Footprint.UNKNOWN_FOOTPRINT;
        }
        if (sqlSessionContext.isInTransStatus()) {
            return Footprint.AUTO_PORT;
        }
        if (sqlSessionContext.getHolder().getZKCache().isInBindMasterPeriod()) {
            return Footprint.MANUAL_REDIRECT2MASTER;
        }
        if (sqlSessionContext.getHolder().getZKCache()
            .isBindMasterSQL(sqlSessionContext.curCmdQuery.getSqlId())) {
            return Footprint.MANUAL_REDIRECT2MASTER;
        }
        if (queryType.isReadOnlyStatement()) {
            return Footprint.REDIRECT2MASTER;
        }
        if (queryType.isWritableStatement()) {
            return Footprint.AUTO_PORT;
        }

        return Footprint.UNKNOWN_FOOTPRINT;
    }

    protected boolean queryCachedResponse(Scheduler scheduler) {
        if (!sqlSessionContext.curCmdQuery.needCacheResult) {
            return false;
        }
        Optional<List<byte[]>> result =
            scheduler.queryResultCache.get(sqlSessionContext.curCmdQuery.queryWithoutComment);
        if (!result.isPresent()) {
            sqlSessionContext.resultPacketsCache =
                Optional.of(new ResultPacketsCache(sqlSessionContext.curCmdQuery));
            return false;
        }
        sqlSessionContext.setState(SESSION_STATUS.QUERY_ANALYZE);
        ByteBuf buf = Unpooled.buffer();
        for (byte[] packet : result.get()) {
            buf.writeBytes(packet);
            sqlSessionContext.writeToClientCounts += packet.length;
        }
        preEndTrace();
        sqlSessionContext.clientWriteAndFlush(buf);
        return true;
    }

    protected boolean isForceQueryMaster() {
        return (sqlSessionContext.bind2master || sqlSessionContext.curCmdQuery.isBindMasterExpr
            || sqlSessionContext.getHolder().getZKCache().isInBindMasterPeriod()
            || sqlSessionContext.getHolder().getZKCache()
            .isBindMasterSQL(sqlSessionContext.curCmdQuery.getSqlId()));
    }

    protected boolean isLogSQLType(byte type) {
        return type != Flags.COM_FIELD_LIST;
    }

    protected String getDBServerID(String dbGroupName, DBRole role) throws QuitException {
        // if no sharding or some error, return old way to getDBGroup
        if (dbGroupName == null) {
            return sqlSessionContext.getHolder().getHomeDbGroup().getDBServer(role);
        }
        // else get the sharding dbserver
        DBGroup g = sqlSessionContext.getHolder().getDbGroup(dbGroupName);
        if (g == null) {
            logger.error("cannot find the DBGroup for sharding DB: " + dbGroupName);
            throw new QuitException("cannot find the DBGroup for sharding DB: " + dbGroupName);
        }
        return g.getDBServer(role);
    }

    protected void extractQuery(CmdQuery query) throws QuitException {
        try {
            query.setWhiteFieldsFilter(
                sqlSessionContext.getHolder().getZKCache().getZKWhiteFieldsFilter());
            query.setShardingRouter(sqlSessionContext.getShardingRouter());
            query.execute();
        } catch (QueryException e) {
            throw new QueryException.Builder(e.errorCode).setSequenceId(1)
                .setErrorMessage(e.errorMessage).bulid(e);
        }
    }

    protected void validateShardingId() throws QueryException {
        if (!sqlSessionContext.isInTransStatus()) {
            return;
        }
        if (sqlSessionContext.getStatus() == SESSION_STATUS.FAKE_SQL) {
            return;
        }
        if (sqlSessionContext.batchCond.isNeedBatchAnalyze()) {
            return;
        }
        boolean isInShardingTransaction = !sqlSessionContext.shardingId.isEmpty();
        if (isInShardingTransaction && !sqlSessionContext.curCmdQuery.queryType
            .isWritableStatement()) {
            return;
        }
        if (isInShardingTransaction ^ sqlSessionContext.isInShardedMode) {
            String errorMessage = Constants.CHAOS_QUERYS_DURING_TRANSACTION;
            logger.error("Not allowed to Mix sharding and non sharding querys during transaction, "
                + sqlSessionContext);
            throw new QueryException.Builder(ErrorCode.ER_COLUMNACCESS_DENIED_ERROR)
                .setSequenceId(1).setErrorMessage(errorMessage).bulid();
        }

        if (sqlSessionContext.batchCond.isShuffledByBatchContext()) {
            /*
             * We do not need the following shardingId comparison in the batch context, as batch context guarantees that all the values within this sql
             * statement are in the same sharding slice.
             *
             * This is to avoid the check when we are in multi-dimension sharding. The SQL sequence is : SET autocommit=0; INSERT INTO test_2d (id, user_id,
             * restaurant_id, user_name, restaurant_name) VALUES (4195732851, 300, 300, 'xxx', 'xxx'), (4196716852, 400, 400, 'yyy', 'yyy'); DELETE FROM test_2d
             * WHERE id = 4196716852;
             *
             * In this multi-value insert, the two values are sharded into the same sharding slice, but before the transaction ends, we are about to delete the
             * second value. Within DAL, it still uses the first value as its shardingId, the `id` in the `delete` statement conflicts with it.
             *
             * As batch context has the guarantee, we return directly here.
             */
            return;
        }

        String currentShardingId = "";
        if (sqlSessionContext.isInShardedMode) {
            currentShardingId = sqlSessionContext.shardingResult.currentResult.id;
        }
        if (isSameShardTransaction(sqlSessionContext.shardingId, currentShardingId)) {
            // 如果前一个shardingId没有包含分隔符,而当前shardingId包含分隔符,则更新shardingId为包含分隔符的那个
            if (!sqlSessionContext.shardingId.contains(SHARDING_ID_DELIMITER) && currentShardingId
                .contains(SHARDING_ID_DELIMITER)) {
                sqlSessionContext.shardingId = currentShardingId;
            }
        } else {
            String errorMessage = Constants.MULTIPLE_KEYS_DURING_TRANSACTION;
            logger.error("Not allowed to sharding table with multiple keys during transaction, "
                + sqlSessionContext);
            throw new QueryException.Builder(ErrorCode.ABORT_CROSS_SHARD_TX).setSequenceId(1)
                .setErrorMessage(errorMessage).bulid();
        }
    }

    private boolean isSameShardTransaction(final String prevShardingId,
        final String currShardingId) {
        //此处不做对参数值的Null防御,因为在方法的调用处前已经过滤掉了参数为Null的情况
        if (prevShardingId.contains(SHARDING_ID_DELIMITER) ^ currShardingId
            .contains(SHARDING_ID_DELIMITER)) {
            String longerShardingId =
                prevShardingId.length() > currShardingId.length() ? prevShardingId : currShardingId;
            String shorterShardingId =
                prevShardingId.length() < currShardingId.length() ? prevShardingId : currShardingId;
            String subShardingId =
                longerShardingId.substring(0, longerShardingId.indexOf(SHARDING_ID_DELIMITER));
            return Objects.equals(shorterShardingId, subShardingId);
        } else {
            return Objects.equals(prevShardingId, currShardingId);
        }
    }

    protected IterableShardingResult generateShardingResultForEndTransQuery(String query) {
        final List<ShardingResult> endTxShardingResults = new ArrayList<ShardingResult>();
        Iterator<ServerSession> it = sqlSessionContext.shardedSession.values().iterator();
        for (int i = 0, j = sqlSessionContext.shardedSession.size();
             i < sqlSessionContext.shardingCount; i++) {
            if (i < j) {
                ServerSession session = it.next();
                ShardingResult endTxQuery =
                    new ShardingResult(query, null, session.getActiveGroup());
                endTxQuery.shardingRuleCount = this.sqlSessionContext.shardingCount;
                endTxQuery.shardingRuleIndex = i;
                endTxQuery.id = this.sqlSessionContext.shardingId;
                endTxShardingResults.add(endTxQuery);
            } else {
                ShardingResult endTxQuery = new ShardingResult(query, null, null);
                endTxQuery.shardingRuleCount = this.sqlSessionContext.shardingCount;
                endTxQuery.shardingRuleIndex = i;
                endTxQuery.id = this.sqlSessionContext.shardingId;
                endTxShardingResults.add(endTxQuery);
            }
        }
        IterableShardingResult shardingResult =
            new SimpleIterableShardingResult(endTxShardingResults);
        return shardingResult;
    }

    protected void executeSetAutoCommitCmd(CmdQuery curCmdQuery) {
        if (curCmdQuery == null) {
            return;
        }
        if (curCmdQuery.shardingSql.autocommitValue == -1) {
            return;
        }
        boolean isAutoCommit4ClientNew = curCmdQuery.shardingSql.autocommitValue == 1;
        if (sqlSessionContext.isInTransStatus()) {
            if (isAutoCommit4ClientNew != sqlSessionContext.transactionController.isAutoCommit()) {
                logger
                    .error("strange! sqlCtx: {} try to set autocommit {} during transaction status",
                        sqlSessionContext, isAutoCommit4ClientNew);
            }
            return;
        }
        sqlSessionContext.transactionController.setAutoCommit(isAutoCommit4ClientNew);
    }

    protected boolean validateReadOnly(CmdQuery curCmdQuery) {
        if (curCmdQuery.queryType.isWritableStatement() && sqlSessionContext.authenticator
            .isReadOnly()) {
            String errorMsg = String
                .format("Not allow execute transaction sql '%s' in read only mode",
                    curCmdQuery.shardingSql.originSQL);
            sqlSessionContext.quitTracer.reportQuit(SessionQuitTracer.QuitTrace.ReadOnlyErr);
            sqlSessionContext.kill(ErrorCode.ABORT_READ_ONLY_MODE, errorMsg);
            return false;
        }
        return true;
    }

    protected void validateRejectSQL(CmdQuery curCmdQuery) {
        if (Objects.isNull(curCmdQuery.shardingSql)) {
            return;
        }
        String sqlPattern = curCmdQuery.shardingSql.getOriginMostSafeSQL();
        String originSQL = sqlSessionContext.getCurQuery();

        // 通过sql pattern拒绝
        boolean isRejectedByPattern =
            sqlSessionContext.getHolder().getZKCache().getSQLByPatternRejecter()
                .rejectSQL(originSQL, sqlPattern, sqlSessionContext.getClientInfo());
        // 通过正则表达式拒绝
        boolean isRejectedByRegularExp =
            sqlSessionContext.getHolder().getZKCache().getSQLByRegularExpRejecter()
                .rejectSQL(originSQL, sqlPattern, sqlSessionContext.getClientInfo());
        boolean isRejectedByRealRegular =
            sqlSessionContext.getHolder().getZKCache().getSQLRealRegularRejecter()
                .rejectSQL(curCmdQuery.queryWithoutComment, sqlPattern,
                    sqlSessionContext.getClientInfo());
        if (isRejectedByPattern || isRejectedByRegularExp || isRejectedByRealRegular) {
            String errorMessage = "A SQL has been rejected:" + curCmdQuery.shardingSql.originSQL;
            throw new QueryException.Builder(ErrorCode.ER_OPTION_PREVENTS_STATEMENT)
                .setSequenceId(1).setErrorMessage(errorMessage).bulid();
        }

    }

    /**
     * 以下是JDBCprepare statement的流程
     * JDBC MYSQL COM_STMT_PREPARE ------------------------ RESPONSE_1 ------------------------
     * 把问号替换为实际参数的SQL ------------------------ RESPONSE_2 ------------------------
     * 但是JDBC有个bug,即使MYSQL在RESPONSE_1里返回ERROR包并且中断连接 JDBC任然会发第二条把问号替换为实际参数的SQL,导致问题 所以需要isJDBCPrepareStmt的字段保存状态 在第二次发送"把问号替换为实际参数的SQL"时再断连接
     * 之所以添加isJDBCPrepareStmt是因为,无法根据第二条sql单独判断是否为COM_STMT_PREPARE
     *
     * @param curCmdQuery
     * @return true 表示该SQL OK,非JDBC Prepare Statement false 表示该SQL非法
     * @throws QueryException
     */
    protected boolean validateJDBCPrepareStmt(CmdQuery curCmdQuery) throws QueryException {
        if (sqlSessionContext.isJDBCPrepareStmt) {
            throw new QueryException.Builder(ErrorCode.ER_SYNTAX_ERROR).setErrorMessage(
                "useServerPrepStmts=true is not allowed in DAL, please remove this parameter!")
                .bulid();
        }
        if (curCmdQuery.queryType == JDBC_PREPARE_STMT) {
            sqlSessionContext.isJDBCPrepareStmt = true;
            logger.error(curCmdQuery.query
                + " try to exeucte prepareStatement but useServerPrepStmts=true is forbidden in DAL");
            sqlSessionContext.setState(SESSION_STATUS.QUERY_ANALYZE);
            sqlSessionContext.clientWriteAndFlush(Unpooled.wrappedBuffer(
                ERR.buildErr(1, ErrorCode.ER_SYNTAX_ERROR.getErrorNo(),
                    "useServerPrepStmts=true is not allowed in DAL, please remove this parameter!")
                    .toPacket()));
            return false;
        }
        return true;
    }

    private boolean validateDispatcher() {
        if (Objects.nonNull(sqlSessionContext.getHolder()) && sqlSessionContext.getHolder()
            .isOffline()) {
            String errorMsg = String.format("sql rejected due to dalgroup %s is offline ",
                sqlSessionContext.getHolder().getDalGroup().getName());
            sqlSessionContext.quitTracer.reportQuit(SessionQuitTracer.QuitTrace.DalGroupOffline);
            sqlSessionContext.kill(ErrorCode.ER_BAD_DB_ERROR, errorMsg);
            return false;
        }
        return true;
    }

    private boolean validateSavepointRelatedQuery() {
        if (isIllegalSavePointRealatedQuery()) {
            String errorMsg =
                "Can't create/release/rollback savepoint before start a transaction and send a writable sql!";
            sqlSessionContext.quitTracer.reportQuit(SessionQuitTracer.QuitTrace.IllegalSavePoint);
            sqlSessionContext.kill(ErrorCode.ERR_SP_IN_NONE_TRANS, errorMsg);
            return false;
        }
        return true;
    }

    public void addDelayReturn(ErrorCode errorCode, String errMsg, String routine) {
        int delayTime = sqlSessionContext.getHolder().getZKCache().getDelayReturnError();
        AthenaServer.quickJobScheduler
            .addOneTimeJob("delay_ret_err_" + sqlSessionContext.getClientAddr(), delayTime, () -> {
                SchedulerWorker.getInstance().enqueue(new DelayResponseManager(sqlSessionContext,
                    sqlSessionContext.buildErr(errorCode, errMsg, routine)));
            });
    }

    protected void preEndTrace() {
        QueryStatistics queryStatistics =
            sqlSessionContext.sqlSessionContextUtil.getQueryStatistics();
        long now = System.currentTimeMillis();
        queryStatistics.setsSendTime(now);
        queryStatistics.setsRecvTime(now);
        queryStatistics.setcSendTime(now);
        long proxyDur = queryStatistics.getProxyDur();
        sqlSessionContext.sqlSessionContextUtil.dbRole = DBRole.DUMMY;
        SQLLog.log(sqlSessionContext.curCmdQuery, sqlSessionContext.fixedSessInfo(),
            sqlSessionContext.getClientInfo(), null, -1, -1, queryStatistics.getProxyTime(), null);
        MetricFactory.newTimer(Metrics.DURATION_CPROXY)
            .addTag(TraceNames.DALGROUP, MetricMetaExtractor.extractDALGroupName(sqlSessionContext))
            .addTag(TraceNames.SQL_TYPE, sqlSessionContext.getQueryType().name())
            .addTag(TraceNames.DB_ROLE, sqlSessionContext.sqlSessionContextUtil.dbRole.name())
            .value(proxyDur);
        sqlSessionContext.sqlSessionContextUtil.resetQueryTimeRecord();
        sqlSessionContext.curCmdQuery = null;
        sqlSessionContext.setState(SESSION_STATUS.QUERY_ANALYZE);
        // 结束etrace跟踪
        sqlSessionContext.sqlSessionContextUtil.endEtrace();
    }

    private Optional<SqlSessionContext> filterSqlCtxByClientId(long clientId4Kill) {
        Predicate<SqlSessionContext> sqlCtxFilter = sqlCtx -> sqlCtx.connectionId == clientId4Kill;
        return HangSessionMonitor.getAllCtx().stream().filter(sqlCtxFilter).findAny();
    }

    /**
     * execute kill cmd
     *
     * @param shardingSQL
     * @see PGClientFakeHandshakeState#handleCancelRequest(byte[]) for postgresql
     */
    protected void excuteKillCmd(ShardingSQL shardingSQL) {
        long clientId4Kill = shardingSQL.getClientIdForKill();
        // 过滤目标SqlCtx
        Optional<SqlSessionContext> sqlCtxOptional = filterSqlCtxByClientId(clientId4Kill);
        // 提前结束当前sqlSessionContext的sql追踪
        preEndTrace();
        // 尝试对目标SqlCtx执行kill操作
        if (!sqlCtxOptional.isPresent()) {
            String errorMessage = String.format("Unknown thread id: %d", clientId4Kill);
            ERR err = ERR.buildErr(1, ErrorCode.ER_NO_SUCH_THREAD.getErrorNo(), errorMessage);
            sqlSessionContext.clientWriteAndFlush(Unpooled.wrappedBuffer(err.toPacket()));
            return;
        }

        SqlSessionContext sqlCtx = sqlCtxOptional.get();
        if (Constants.ATHENA_ADMIN.equals(sqlSessionContext.authenticator.userName) || Objects
            .equals(sqlSessionContext.authenticator.userName, sqlCtx.authenticator.userName)) {
            String errMsg = String
                .format("Session is killed by %s with clientConnId=%d use kill cmd",
                    sqlSessionContext.authenticator.userName, sqlSessionContext.connectionId);
            SchedulerWorker.getInstance().enqueue(
                new ManualKillerManager(sqlCtx, SessionQuitTracer.QuitTrace.ManualKill,
                    new ResponseStatus(ResponseStatus.ResponseType.ABORT,
                        ErrorCode.ABORT_MANUAL_KILL, errMsg), ErrorCode.ABORT_MANUAL_KILL, errMsg));

            if (Objects.equals(sqlSessionContext, sqlCtx)) {
                ERR err = ERR.buildErr(1, ErrorCode.ER_QUERY_INTERRUPTED.getErrorNo(), "70100",
                    "Query execution was interrupted");
                sqlSessionContext.clientWriteAndFlush(Unpooled.wrappedBuffer(err.toPacket()));
            } else {
                sqlSessionContext
                    .clientWriteAndFlush(Unpooled.wrappedBuffer(sqlSessionContext.buildOk()));
            }
            return;
        }

        String errorMessage = String.format("You are not owner of thread %d", clientId4Kill);
        ERR err = ERR.buildErr(1, ErrorCode.ER_KILL_DENIED_ERROR.getErrorNo(), errorMessage);
        sqlSessionContext.clientWriteAndFlush(Unpooled.wrappedBuffer(err.toPacket()));
    }

    /**
     * 如果ZK DALGroup节点下`manual_rewrite_index_hints`有有效的配置,则对匹配的SQL增加
     * <a href="https://dev.mysql.com/doc/refman/5.7/en/index-hints.html">index hints</a>
     * 该功能只有MySQL有,PostgreSQL语法上不支持
     *
     * @param cmdQuery
     */
    protected void rewriteIndexHintsIfConfigured(CmdQuery cmdQuery) {
        if (Objects.isNull(cmdQuery.shardingSql)) {
            return;
        }
        if (!sqlSessionContext.getHolder().getZKCache()
            .isAppendIndexHintsSQL(cmdQuery.getSqlId())) {
            return;
        }
        ZKSQLHint sqlhint = sqlSessionContext.getHolder().getZKCache()
            .getAppendIndexHintsBySQLHash(cmdQuery.getSqlId());
        boolean rewrited = cmdQuery.shardingSql.rewriteSQLHints(sqlhint.hints);
        if (!rewrited) {
            return;
        }
        Com_Query query = new Com_Query();
        query.setQuery(
            ShardingSQL.addPrefixComment(cmdQuery.queryComment, cmdQuery.shardingSql.originSQL));
        cmdQuery.rewriteBytes(query.toPacket());
        cmdQuery.setIndexHints(sqlhint.hintsStr);
    }
}
