package me.ele.jarch.athena.netty;

import com.github.mpjct.jmpjct.mysql.proto.OK;
import com.github.mpjct.jmpjct.util.ErrorCode;
import io.netty.buffer.Unpooled;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.constant.EventTypes;
import me.ele.jarch.athena.constant.Metrics;
import me.ele.jarch.athena.constant.TraceNames;
import me.ele.jarch.athena.exception.QueryException;
import me.ele.jarch.athena.server.pool.ServerSession;
import me.ele.jarch.athena.sharding.MysqlShardingResultSet;
import me.ele.jarch.athena.sharding.ShardingResultSet;
import me.ele.jarch.athena.sharding.sql.IterableShardingResult;
import me.ele.jarch.athena.sql.QUERY_TYPE;
import me.ele.jarch.athena.sql.QueryResultContext;
import me.ele.jarch.athena.sql.ResultType;
import me.ele.jarch.athena.sql.ShardingQueryResultContext;
import me.ele.jarch.athena.util.AggregateFunc;
import me.ele.jarch.athena.util.JacksonObjectMappers;
import me.ele.jarch.athena.util.SafeJSONHelper;
import me.ele.jarch.athena.util.etrace.MetricFactory;
import me.ele.jarch.athena.util.etrace.TraceEnhancer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ShardingContext {
    private static final Logger logger = LoggerFactory.getLogger(ShardingContext.class);

    protected QUERY_TYPE queryType = QUERY_TYPE.OTHER;
    protected String originalTable = "unknowntable";
    protected int sentRows = 0;
    public ShardingResultSet selectResultSet = null;
    protected int selectResultSetBufSize = 0;
    protected boolean preReturn = false;
    protected List<AggregateFunc> aggrMethods = new ArrayList<>();
    protected boolean isAggrMethods = false;
    protected boolean interruptToReturn = false;
    protected List<byte[]> sendBuf = new ArrayList<>();
    // TODO remove unused variable
    protected int count;
    protected SessionQuitTracer quitTracer;
    protected List<byte[]> firstOKBytes;
    protected Map<String, Set<String>> originShardingKey = Collections.emptyMap();

    public void init(IterableShardingResult shardingResult, String originalTable,
        QUERY_TYPE queryType) {
        this.reset();
        this.originalTable = originalTable;
        this.queryType = queryType;
        this.aggrMethods = shardingResult.aggrMethods;
        this.originShardingKey = shardingResult.originShardingKey;
        aggrMethods.forEach(method -> {
            if (method.equals(AggregateFunc.COUNT) || method.equals(AggregateFunc.SUM) || method
                .equals(AggregateFunc.MAX) || method.equals(AggregateFunc.MIN)) {
                isAggrMethods = true;
            }
        });
        selectResultSet = newShardingResultSet(shardingResult.aggrMethods,
            !shardingResult.groupByItems.isEmpty());
    }

    protected ShardingResultSet newShardingResultSet(List<AggregateFunc> columnAggeTypes,
        boolean isGroupBy) {
        return new MysqlShardingResultSet(columnAggeTypes, isGroupBy);
    }

    public ShardingContext(SessionQuitTracer quitTracer) {
        this.quitTracer = quitTracer;
    }

    public void appendQueryResult(ShardingQueryResultContext reCtx, int shardingRuleIndex,
        int limit) {
        if (this.interruptToReturn) {
            return;
        }

        switch (queryType) {
            case SELECT_FOR_UPDATE:
            case SHOW_CMD:
            case SELECT: {

                if (reCtx.resultType != ResultType.RESULT_SET) {
                    logger.error(
                        "ResultType is not RESULT_SET,ResultType=" + reCtx.resultType.name());
                    this.interruptToReturn = true;
                    if (selectResultSet == null) {
                        // appendQueryResult was called first time
                        sendBuf = reCtx.shardingBuf;
                    } else {
                        selectResultSet.rows.addAll(reCtx.responseClassifier.rows);
                        // add ERR Packet to ResultSet.rows
                        selectResultSet.rows.addAll(reCtx.shardingBuf);
                        sendBuf = selectResultSet.toPartPackets(false);
                    }
                    break;
                }

                selectResultSet
                    .addColumn(reCtx.responseClassifier.columns, reCtx.getShardingIndex());
                selectResultSet.addRow(reCtx.responseClassifier.rows, reCtx.getShardingIndex());
                selectResultSetBufSize = selectResultSet.getSelectResultSetBufSize();
                count += reCtx.getColumBufSize() + reCtx.getRowBufSize();

                // if we got enough rows which is equal to limit
                if (limit != -1 && selectResultSet.rows.size() >= (limit - this.sentRows)) {
                    this.interruptToReturn = true;
                    selectResultSet.rows = selectResultSet.rows.subList(0, limit - this.sentRows);
                    sendBuf = selectResultSet.toPartPackets(true);
                } else if (this.selectResultSetBufSize > QueryResultContext.SENT_BUFFER_SIZE
                    && !isAggrMethods) {
                    this.preReturn = true;
                }

                break;
            }
            case UPDATE:
            case REPLACE:
            case INSERT:
            case DELETE: {
                if (reCtx.resultType != ResultType.OK) {
                    this.interruptToReturn = true;
                    sendBuf.clear();
                    // send ERR Packet
                    sendBuf.addAll(reCtx.shardingBuf);
                    break;
                }

                if (shardingRuleIndex == 0) {
                    sendBuf.addAll(reCtx.shardingBuf);
                    firstOKBytes = reCtx.shardingBuf;
                }

                if (shardingRuleIndex == 1) {
                    compareOK(reCtx);
                }
                break;
            }
            case COMMIT: {
                if (shardingRuleIndex == 0) {
                    sendBuf.addAll(reCtx.shardingBuf);
                    if (reCtx.resultType != ResultType.OK) {
                        this.interruptToReturn = true;
                    }
                    firstOKBytes = reCtx.shardingBuf;
                } else if (shardingRuleIndex == 1) {
                    // 第二个commit返回
                    if (reCtx.sqlCtx != null) {
                        reCtx.sqlCtx.sqlSessionContextUtil.secondCommitId++;
                    }
                    if (reCtx.resultType != ResultType.OK) {
                        this.interruptToReturn = true;
                        MetricFactory.newCounterWithSqlSessionContext(Metrics.SECOND_COMMIT_FAIL,
                            reCtx.sqlCtx).once();
                        if (reCtx.sqlCtx != null) {
                            String errLog = "second commit failure," + reCtx.sqlCtx + ", Query :";
                            if (reCtx.sqlCtx.curCmdQuery != null) {
                                errLog = errLog + reCtx.sqlCtx.curCmdQuery.query;
                            }
                            logger.error(errLog);
                        }
                    }
                    compareOK(reCtx);
                }
                break;
            }
            default: {
                if (queryType == QUERY_TYPE.ROLLBACK && shardingRuleIndex == 1
                    && reCtx.sqlCtx != null) {
                    reCtx.sqlCtx.sqlSessionContextUtil.secondCommitId++;
                }
                if (shardingRuleIndex == 0) {
                    sendBuf.addAll(reCtx.shardingBuf);
                }
                break;
            }
        }
    }

    protected void compareOK(ShardingQueryResultContext reCtx) {
        try {
            OK firstOK = loadOK(firstOKBytes);
            OK secOK = loadOK(reCtx.shardingBuf);
            if (secOK.affectedRows != firstOK.affectedRows) {
                MetricFactory.newCounter(Metrics.AFFECT_ROWS_DIFF)
                    .addTag(TraceNames.TABLE, originalTable).once();
                TraceEnhancer.newFluentEvent(EventTypes.TWO_DIM_DML_DIFF, originalTable).data(
                    SafeJSONHelper.of(JacksonObjectMappers.getMapper())
                        .writeValueAsStringOrDefault(originShardingKey, "{}")).status("DATA_UNSYNC")
                    .complete();
                logger.error(
                    "The affectedRows from user database and from restaurant database is different."
                        + reCtx.sqlCtx.curCmdQuery.query);
            }
        } catch (Exception t) {
            logger.error("OK.compareOK", t);
        }
    }

    private OK loadOK(List<byte[]> okBytes) {
        OK ok = OK.loadFromPacket(OK.getOkPacketBytes());
        try {
            ok = OK.loadFromPacket(okBytes.get(0));
        } catch (Exception t) {
            logger.error("OK.loadOK", t);
        }
        return ok;
    }

    public byte[][] getMergedReuslt() {
        return getMergedReuslt(null);
    }

    public byte[][] getMergedReuslt(SqlSessionContext sqlSessionContext) {
        List<byte[]> result = new ArrayList<>();
        if (queryType == QUERY_TYPE.SELECT || queryType == QUERY_TYPE.SELECT_FOR_UPDATE
            || queryType == QUERY_TYPE.SHOW_CMD) {
            if (this.interruptToReturn) {
                result = sendBuf;
            } else {
                result = selectResultSet.toPartPackets(true);
            }
        } else {
            result = sendBuf;
        }
        reset();
        if (sqlSessionContext != null) {
            for (byte[] arr : result) {
                sqlSessionContext.writeToClientCounts += arr.length;
            }
        }
        return result.toArray(new byte[0][]);
    }

    protected void reset() {
        queryType = QUERY_TYPE.OTHER;
        selectResultSet = null;
        interruptToReturn = false;
        sendBuf = new ArrayList<>();
        aggrMethods = Collections.emptyList();
        isAggrMethods = false;
        sentRows = 0;
        preReturn = false;
    }

    public boolean isInterruptToReturn() {
        return interruptToReturn;
    }

    public boolean isPreReturn() {
        return preReturn;
    }

    public byte[][] getPreSendBuf() {
        return getPreSendBuf(null);
    }

    public byte[][] getPreSendBuf(SqlSessionContext sqlSessionContext) {
        List<byte[]> preSendBuf = new ArrayList<>();
        this.sentRows = this.sentRows + selectResultSet.rows.size();
        preSendBuf.addAll(selectResultSet.toPartPackets(false));
        preReturn = false;
        this.selectResultSetBufSize = 0;
        if (sqlSessionContext != null) {
            for (byte[] arr : preSendBuf) {
                sqlSessionContext.writeToClientCounts += arr.length;
            }
        }
        return preSendBuf.toArray(new byte[0][]);
    }

    public void writeAndFlushIfNeedBeforeDone(ShardingQueryResultContext sqrc) {
        int currentCount = sqrc.getColumBufSize() + sqrc.getRowBufSize();
        if (currentCount > Constants.MAX_AUTOREAD_TRIGGER_SIZE) {
            if (sqrc.responseClassifier.rows.size() == 0) {
                long sequenceId = this.selectResultSet.sequenceId;
                this.selectResultSet = null;
                quitTracer.reportQuit(SessionQuitTracer.QuitTrace.DasMaxResultBufBroken);
                throw new QueryException.Builder(ErrorCode.ERR_OVER_MAX_RESULT_BUF_SIZE)
                    .setSequenceId(sequenceId).setErrorMessage(Constants.OVER_MAX_RESULT_BUF_SIZE)
                    .bulid();
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

    public boolean needAutoReadControll(boolean isShardingMode) {
        return isShardingMode && (queryType == QUERY_TYPE.SELECT
            || queryType == QUERY_TYPE.SELECT_FOR_UPDATE || queryType == QUERY_TYPE.SHOW_CMD)
            && !interruptToReturn && !isAggrMethods;
    }
}
