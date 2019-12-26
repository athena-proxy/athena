package me.ele.jarch.athena.netty;

import com.github.mpjct.jmpjct.util.ErrorCode;
import io.netty.buffer.Unpooled;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.constant.Metrics;
import me.ele.jarch.athena.constant.TraceNames;
import me.ele.jarch.athena.exception.QueryException;
import me.ele.jarch.athena.server.pool.ServerSession;
import me.ele.jarch.athena.sharding.PGShardingResultSet;
import me.ele.jarch.athena.sharding.ShardingResultSet;
import me.ele.jarch.athena.sql.QUERY_TYPE;
import me.ele.jarch.athena.sql.QueryResultContext;
import me.ele.jarch.athena.sql.ResultType;
import me.ele.jarch.athena.sql.ShardingQueryResultContext;
import me.ele.jarch.athena.util.AggregateFunc;
import me.ele.jarch.athena.util.etrace.MetricFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Created by jinghao.wang on 17/7/17.
 */
public class PGShardingContext extends ShardingContext {
    private static final Logger LOG = LoggerFactory.getLogger(PGShardingContext.class);

    public PGShardingContext(SessionQuitTracer quitTracer) {
        super(quitTracer);
    }

    @Override protected ShardingResultSet newShardingResultSet(List<AggregateFunc> columnAggeTypes,
        boolean isGroupBy) {
        return new PGShardingResultSet(columnAggeTypes, isGroupBy);
    }

    private void appendResultSet(ShardingQueryResultContext reCtx, int limit) {
        if (reCtx.resultType != ResultType.RESULT_SET) {
            // SQL的结果类型不符合预期的异常处理
            if (Objects.isNull(reCtx.responseClassifier.getReadyForQuery())) {
                // 对于PostgreSQL协议,如果查询失败,则必须等待`ReadyForQuery`数据包到来, 才能返回。
                return;
            }
            LOG.error("ResultType is not RESULT_SET,ResultType={}", reCtx.resultType.name());
            interruptToReturn = true;
            if (Objects.isNull(selectResultSet)) {
                // appendQueryResult was called first time
                sendBuf = reCtx.shardingBuf;
            } else {
                selectResultSet.rows.addAll(reCtx.responseClassifier.rows);
                // add ERR Packet to ResultSet.rows
                selectResultSet.rows.addAll(reCtx.shardingBuf);
                sendBuf = selectResultSet.toPartPackets(false);
            }
            return;
        }

        // SQL的结果类型符合预期为RESULT_SET
        selectResultSet.addColumn(reCtx.responseClassifier.columns, reCtx.getShardingIndex());
        selectResultSet.addRow(reCtx.responseClassifier.rows, reCtx.getShardingIndex());
        reCtx.responseClassifier.getCommandComplete()
            .ifPresent(selectResultSet::addCommandComplete);
        reCtx.responseClassifier.getReadyForQuery().ifPresent(selectResultSet::addReadyForQuery);
        selectResultSetBufSize = selectResultSet.getSelectResultSetBufSize();
        count += reCtx.getColumBufSize() + reCtx.getRowBufSize();

        // if we got enough rows which is equal to limit and has at least one complete response
        if (isEnoughRows4Sent(limit) && hasCompleteResponse()) {
            // we must wait for commandComplete and readyForQuery packets for PostgreSQL protocol,
            // even if we got enough limit rows.
            interruptToReturn = true;
            selectResultSet.rows = selectResultSet.rows.subList(0, limit - this.sentRows);
            sendBuf = selectResultSet.toPartPackets(true);
        } else if (selectResultSetBufSize > QueryResultContext.SENT_BUFFER_SIZE && !isAggrMethods) {
            preReturn = true;
        }
    }

    private boolean isEnoughRows4Sent(int limit) {
        return limit != -1 && selectResultSet.rows.size() >= (limit - sentRows);
    }

    private boolean hasCompleteResponse() {
        return !selectResultSet.commandCompletes.isEmpty() && !selectResultSet.readyForQuerys
            .isEmpty();
    }

    private void appendOk(ShardingQueryResultContext reCtx, int shardingRuleIndex) {
        if (reCtx.resultType != ResultType.OK && reCtx.responseClassifier.getReadyForQuery()
            .isPresent()) {
            interruptToReturn = true;
            sendBuf.clear();
            // send ERR Packet
            sendBuf.addAll(reCtx.shardingBuf);
            return;
        }

        Optional<byte[]> readyForQueryOp = reCtx.responseClassifier.getReadyForQuery();
        // 响应结果都是Ok
        if (shardingRuleIndex == 0 && readyForQueryOp.isPresent()) {
            sendBuf.addAll(reCtx.shardingBuf);
            firstOKBytes = reCtx.shardingBuf;
        }
        if (shardingRuleIndex == 1 && readyForQueryOp.isPresent()) {
            compareOK(reCtx);
        }
    }

    private void appendCommitOk(ShardingQueryResultContext reCtx, int shardingRuleIndex) {
        Optional<byte[]> readyForQueryOp = reCtx.responseClassifier.getReadyForQuery();
        if (shardingRuleIndex == 0 && readyForQueryOp.isPresent()) {
            sendBuf.addAll(reCtx.shardingBuf);
            if (reCtx.resultType != ResultType.OK) {
                interruptToReturn = true;
            }
            firstOKBytes = reCtx.shardingBuf;
        } else if (shardingRuleIndex == 1 && readyForQueryOp.isPresent()) {
            // 第二个commit返回
            if (reCtx.sqlCtx != null) {
                reCtx.sqlCtx.sqlSessionContextUtil.secondCommitId++;
            }
            if (reCtx.resultType != ResultType.OK) {
                interruptToReturn = true;
                MetricFactory
                    .newCounterWithSqlSessionContext(Metrics.SECOND_COMMIT_FAIL, reCtx.sqlCtx)
                    .once();
                if (reCtx.sqlCtx != null) {
                    String errLog = "second commit failure," + reCtx.sqlCtx + ", Query :";
                    if (reCtx.sqlCtx.curCmdQuery != null) {
                        errLog = errLog + reCtx.sqlCtx.curCmdQuery.query;
                    }
                    LOG.error(errLog);
                }
            }
            compareOK(reCtx);
        }
    }

    @Override public void appendQueryResult(ShardingQueryResultContext reCtx, int shardingRuleIndex,
        int limit) {
        if (interruptToReturn) {
            return;
        }

        switch (queryType) {
            case SELECT_FOR_UPDATE:
            case SHOW_CMD:
            case SELECT:
                appendResultSet(reCtx, limit);
                break;
            case UPDATE:
            case REPLACE:
            case INSERT:
            case DELETE:
                appendOk(reCtx, shardingRuleIndex);
                break;
            case COMMIT:
                appendCommitOk(reCtx, shardingRuleIndex);
                break;
            default: {
                if (queryType == QUERY_TYPE.ROLLBACK && shardingRuleIndex == 1
                    && reCtx.sqlCtx != null) {
                    reCtx.sqlCtx.sqlSessionContextUtil.secondCommitId++;
                }
                if (shardingRuleIndex == 0 && reCtx.responseClassifier.getReadyForQuery()
                    .isPresent()) {
                    sendBuf.addAll(reCtx.shardingBuf);
                }
                break;
            }
        }
    }

    @Override protected void compareOK(ShardingQueryResultContext reCtx) {
        try {
            List<byte[]> secOK = reCtx.shardingBuf;
            if (firstOKBytes.size() != secOK.size()) {
                MetricFactory.newCounter(Metrics.AFFECT_ROWS_DIFF)
                    .addTag(TraceNames.TABLE, originalTable).once();
                LOG.error(
                    "The affectedRows from user database and from restaurant database is different."
                        + reCtx.sqlCtx.curCmdQuery.query);
            }
            for (int i = 0; i < firstOKBytes.size(); i++) {
                if (!Arrays.equals(firstOKBytes.get(i), secOK.get(i))) {
                    MetricFactory.newCounter(Metrics.AFFECT_ROWS_DIFF).once();
                    LOG.error(
                        "The affectedRows from user database and from restaurant database is different."
                            + reCtx.sqlCtx.curCmdQuery.query);
                }
            }
        } catch (Exception e) {
            LOG.error("OK.comparOK", e);
        }
    }

    @Override public void writeAndFlushIfNeedBeforeDone(ShardingQueryResultContext sqrc) {
        int currentCount = sqrc.getColumBufSize() + sqrc.getRowBufSize();
        if (currentCount > Constants.MAX_AUTOREAD_TRIGGER_SIZE) {
            if (sqrc.responseClassifier.rows.size() == 0) {
                this.selectResultSet = null;
                quitTracer.reportQuit(SessionQuitTracer.QuitTrace.DasMaxResultBufBroken);
                throw new QueryException.Builder(ErrorCode.ERR_OVER_MAX_RESULT_BUF_SIZE)
                    .setErrorMessage(Constants.OVER_MAX_RESULT_BUF_SIZE).bulid();
            }

            appendQueryResult(sqrc, sqrc.sqlCtx.shardingResult.currentResult.shardingRuleIndex,
                sqrc.sqlCtx.shardingResult.currentResult.limit);
            if (sqrc.sqlCtx.shardingContext.isPreReturn()) {
                ServerSession serverSession =
                    sqrc.sqlCtx.shardedSession.get(sqrc.sqlCtx.scheduler.getInfo().getGroup());
                sqrc.sqlCtx.clientWriteAndFlushWithTrafficControll(
                    Unpooled.wrappedBuffer(sqrc.sqlCtx.shardingContext.getPreSendBuf(sqrc.sqlCtx)),
                    serverSession);
            }
            sqrc.resetColumnBuf();
            sqrc.resetRowBuf();
        }
    }
}
