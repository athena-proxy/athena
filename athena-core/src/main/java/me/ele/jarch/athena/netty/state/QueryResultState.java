package me.ele.jarch.athena.netty.state;

import com.github.mpjct.jmpjct.mysql.proto.ERR;
import io.netty.buffer.Unpooled;
import me.ele.jarch.athena.allinone.DBRole;
import me.ele.jarch.athena.constant.Metrics;
import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.sharding.sql.SimpleIterableShardingResult;
import me.ele.jarch.athena.sql.*;
import me.ele.jarch.athena.util.NoThrow;
import me.ele.jarch.athena.util.etrace.MetricFactory;
import me.ele.jarch.athena.util.rmq.RmqSqlInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryResultState implements State {

    private static final Logger logger = LoggerFactory.getLogger(QueryResultState.class);
    private static final String WRAPPED_ERR_TEMPLATE = "target table: %s, %s";
    protected SqlSessionContext sqlSessionContext;
    protected volatile CmdQueryResult result = null;

    public QueryResultState(SqlSessionContext sqlSessionContext) {
        this.sqlSessionContext = sqlSessionContext;
    }

    @Override public boolean handle() throws QuitException {
        return doQueryResult();
    }

    protected CmdQueryResult newCmdQueryResult(QueryResultContext ctx) {
        return new CmdQueryResult(ctx);
    }

    protected boolean doQueryResult() throws QuitException {
        if (!sqlSessionContext.isSwallowQueryResponse()) {
            sqlSessionContext.queryId = -1;
        }
        if (result == null) {
            QueryResultContext ctx = buildQueryResultContext();
            ctx.packets = sqlSessionContext.dbServerPackets;
            ctx.sqlCtx = sqlSessionContext;
            result = newCmdQueryResult(ctx);
        } else {
            result.getCtx().packets = sqlSessionContext.dbServerPackets;
        }
        result.execute();

        //检查当前未完成的sql查询是否需要autoread控制
        if (!result.isDone() && !sqlSessionContext.isInMappingShardedMode
            && sqlSessionContext.shardingContext
            .needAutoReadControll(sqlSessionContext.isInShardedMode)) {
            //获取当前sharding sql的context
            //此处只处理响应类型为RESULT_SET的结果
            ShardingQueryResultContext sqrc = (ShardingQueryResultContext) result.getCtx();
            sqlSessionContext.shardingContext.writeAndFlushIfNeedBeforeDone(sqrc);
        }
        if (result.isDone()) {
            if (sqlSessionContext.isSwallowQueryResponse()) {
                // mysql协议永远不会进入此语句块
                result = null;
                onQueryResponseSwallowed();
                if (!sqlSessionContext.dbServerPackets.isEmpty()) {
                    // 可能收到1+个queryResponse
                    sqlSessionContext.getHolder().enqueue(sqlSessionContext);
                }
                return false;
            }
            try {
                sqlSessionContext.sqlSessionContextUtil.getCurrentQueryStatistics()
                    .setcSendTime(System.currentTimeMillis());
                sqlSessionContext.sqlSessionContextUtil.doStatistics();
                // 结束etrace对dal连接数据库的跟踪事务
                sqlSessionContext.sqlSessionContextUtil.completeTransaction();
                NoThrow.call(() -> RmqSqlInfo.setCommonInfo(sqlSessionContext));
                sqlSessionContext.returnResource();
                handleResultDone();
            } finally {
                result = null;
                sqlSessionContext.releaseDbSemaphore();
            }

            if (sqlSessionContext.grayUp.isStarted()) {
                doGray();
            }
            if (hasUnsyncQuery()) {
                sqlSessionContext.getHolder().enqueue(sqlSessionContext);
            }
        }

        return false;
    }

    protected boolean hasUnsyncQuery() {
        return sqlSessionContext.allowUnsyncQuery();
    }

    protected void onQueryResponseSwallowed() {
        throw new IllegalStateException("mysql protocol should not swallow db response");
    }

    private void doGray() {
        if (sqlSessionContext.curCmdQuery.queryType.isTransStatement()) {
            try {
                String id = sqlSessionContext.getHolder().getHomeDbGroup().getDBServer(DBRole.GRAY);
                sqlSessionContext.scheduler = sqlSessionContext.getHolder().getSched(id);
                sqlSessionContext.setState(SESSION_STATUS.GRAY_QUERY_HANDLE);
                sqlSessionContext.getHolder().enqueue(sqlSessionContext);
                sqlSessionContext.grayUp.isReadySend = true;
                sqlSessionContext.sqlSessionContextUtil
                    .startGrayQueryStatistics(sqlSessionContext.curCmdQuery);
                return;
            } catch (Exception e) {
                logger.error("doGray failed " + sqlSessionContext, e);
                sqlSessionContext.grayUp.isReadySend = false;
                sqlSessionContext.grayUp.clearGrayUp();
            }
        }
        sqlSessionContext.curCmdQuery = null;
        // 结束etrace跟踪
        sqlSessionContext.sqlSessionContextUtil.endEtrace();
        sqlSessionContext.setState(SESSION_STATUS.QUERY_ANALYZE);
        sqlSessionContext.clientWriteAndFlush(Unpooled.EMPTY_BUFFER);
    }

    @Override public SESSION_STATUS getStatus() {
        return SESSION_STATUS.QUERY_RESULT;
    }

    private QueryResultContext buildQueryResultContext() {
        if (sqlSessionContext.isSwallowQueryResponse()) {
            // Mysql Protocol never run this code block
            return newDummyQueryResultContext();
        }
        if (!sqlSessionContext.isInShardedMode) {
            return newNonShardingQueryResultContext();
        }
        if (sqlSessionContext.isInMappingShardedMode) {
            return newInterceptQueryResultContext();
        }
        ShardingQueryResultContext shardingCtx = newShardingQueryResultContext();
        shardingCtx.setShardingIndex(sqlSessionContext.shardingResult.getIndex());
        return shardingCtx;
    }

    protected QueryResultContext newDummyQueryResultContext() {
        throw new IllegalStateException(
            "strange! MySQL protocol dose not support swallow query response");
    }

    protected QueryResultContext newNonShardingQueryResultContext() {
        return new QueryResultContext();
    }

    protected QueryResultContext newInterceptQueryResultContext() {
        return new InterceptQueryResult();
    }

    protected ShardingQueryResultContext newShardingQueryResultContext() {
        return new ShardingQueryResultContext();
    }

    private static byte[] buildWrappedErr(ERR err, String originalTable) {
        long errorCode = err.errorCode;
        String sqlState = err.sqlState;
        String errorMessage = String.format(WRAPPED_ERR_TEMPLATE, originalTable, err.errorMessage);
        ERR wrappedErr = ERR.buildErr(1, errorCode, sqlState, errorMessage);
        return wrappedErr.toPacket();
    }

    /**
     * 只用于mapping sql执行得到结果后的结果判定
     *
     * @param resultType
     * @return
     */
    private boolean isResultTypeAsExpected(ResultType resultType) {
        QUERY_TYPE queryType = sqlSessionContext.curCmdQuery.queryType;
        if (queryType == QUERY_TYPE.INSERT && resultType == ResultType.OK) {
            return true;
        }
        if (queryType == QUERY_TYPE.SELECT && resultType == ResultType.RESULT_SET) {
            return true;
        }
        if (queryType == QUERY_TYPE.UPDATE && resultType == ResultType.RESULT_SET) {
            return true;
        }
        if (queryType == QUERY_TYPE.DELETE && resultType == ResultType.RESULT_SET) {
            return true;
        }
        if (queryType == QUERY_TYPE.SELECT_FOR_UPDATE && resultType == ResultType.RESULT_SET) {
            return true;
        }
        return false;
    }

    private void handleResultDone() throws QuitException {
        //非sharding逻辑
        if (!sqlSessionContext.isInShardedMode) {
            sqlSessionContext.isInShardedMode = false;
            if (!sqlSessionContext.grayUp.isStarted()) {
                sqlSessionContext.curCmdQuery = null;
                // 结束etrace跟踪
                sqlSessionContext.sqlSessionContextUtil.endEtrace();
                sqlSessionContext.setState(SESSION_STATUS.QUERY_ANALYZE);
                sqlSessionContext.clientWriteAndFlush(Unpooled.EMPTY_BUFFER);
                sqlSessionContext.trySaveResultPacketsCache();
            }
            return;
        }
        //sharding 非mapping sharding 逻辑
        if (!sqlSessionContext.isInMappingShardedMode) {
            sqlSessionContext.shardingContext
                .appendQueryResult((ShardingQueryResultContext) result.getCtx(),
                    sqlSessionContext.shardingResult.currentResult.shardingRuleIndex,
                    sqlSessionContext.shardingResult.currentResult.limit);
            if (!sqlSessionContext.shardingResult.hasNext() || sqlSessionContext.shardingContext
                .isInterruptToReturn()) {
                sqlSessionContext.sqlSessionContextUtil.endAllShardingQueryStatistics();
                sqlSessionContext.shardingResult = new SimpleIterableShardingResult();
                sqlSessionContext.returnResource();
                sqlSessionContext.curCmdQuery = null;
                sqlSessionContext.isInShardedMode = false;
                sqlSessionContext.setState(SESSION_STATUS.QUERY_ANALYZE);
                sqlSessionContext.clientWriteAndFlush(Unpooled.wrappedBuffer(
                    sqlSessionContext.shardingContext.getMergedReuslt(sqlSessionContext)));
                // 结束etrace跟踪
                sqlSessionContext.sqlSessionContextUtil.endEtrace();
            } else {
                if (sqlSessionContext.shardingContext.isPreReturn()) {
                    sqlSessionContext.clientWriteAndFlush(
                        Unpooled.wrappedBuffer(sqlSessionContext.shardingContext.getPreSendBuf()));
                }
                sqlSessionContext.setState(SESSION_STATUS.QUERY_ANALYZE);
                sqlSessionContext.getHolder().enqueue(sqlSessionContext);
            }
            return;
        }
        //sharding mapping sharding 逻辑
        //结束对映射sql的追踪
        sqlSessionContext.sqlSessionContextUtil.endMappingEtrace();
        QueryResultContext ctx = result.getCtx();
        InterceptQueryResult interceptQueryResult = (InterceptQueryResult) ctx;
        if (interceptQueryResult.resultType == ResultType.ERR) {
            String originalTable = sqlSessionContext.curCmdQuery.originalTable;
            byte[] wrappedErr = buildWrappedErr(interceptQueryResult.err, originalTable);
            sqlSessionContext.sqlSessionContextUtil.endAllShardingQueryStatistics();
            sqlSessionContext.shardingResult = new SimpleIterableShardingResult();
            sqlSessionContext.returnResource();
            sqlSessionContext.curCmdQuery = null;
            sqlSessionContext.isInShardedMode = false;
            sqlSessionContext.isInMappingShardedMode = false;
            sqlSessionContext.sqlSessionContextUtil.endEtrace();
            sqlSessionContext.setState(SESSION_STATUS.QUERY_ANALYZE);
            sqlSessionContext.clientWriteAndFlush(Unpooled.wrappedBuffer(wrappedErr));
            return;
        } else if (interceptQueryResult.resultType == ResultType.OK && isResultTypeAsExpected(
            interceptQueryResult.resultType)) {
        } else if (interceptQueryResult.resultType == ResultType.RESULT_SET
            && isResultTypeAsExpected(interceptQueryResult.resultType)) {
            ResultSet rs = new ResultSet(interceptQueryResult.responseClassifier,
                interceptQueryResult.resultType, interceptQueryResult.err, null);
            if (rs.next()) {
                sqlSessionContext.curCmdQuery.shardingSql.collectShardingColumnValue(
                    sqlSessionContext.shardingResult.currentResult.shardingRuleIndex,
                    rs.getString(1));
            } else {
                logger.warn(
                    "empty result set when execute mapping sql {}, clientConnId={}, transId={}",
                    sqlSessionContext.getCurQuery(), sqlSessionContext.connectionId,
                    sqlSessionContext.transactionId);
                MetricFactory.newCounterWithSqlSessionContext(Metrics.MAPPING_SQL_AFFECTED_ZERO,
                    sqlSessionContext).once();
            }
        } else {
            throw new QuitException(String
                .format("unknown result type: %s in mapping sharding mode",
                    interceptQueryResult.resultType));
        }
        if (!sqlSessionContext.shardingResult.hasNext()) {
            sqlSessionContext.isInMappingShardedMode = false;
            sqlSessionContext.curCmdQuery.shardingSql.generateOriginalShardedSQLs();
            sqlSessionContext.initSharingContext(sqlSessionContext.curCmdQuery.shardingSql.results);
        }
        sqlSessionContext.setState(SESSION_STATUS.QUERY_ANALYZE);
        sqlSessionContext.getHolder().enqueue(sqlSessionContext);
    }
}
