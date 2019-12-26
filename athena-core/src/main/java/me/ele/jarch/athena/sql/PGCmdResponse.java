package me.ele.jarch.athena.sql;

import com.github.mpjct.jmpjct.util.ErrorCode;
import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.pg.proto.*;
import me.ele.jarch.athena.util.ResponseStatus;
import me.ele.jarch.athena.util.etrace.DalMultiMessageProducer;

import java.util.Arrays;
import java.util.Objects;

/**
 * Created by jinghao.wang on 16/11/28.
 */
public class PGCmdResponse extends ComposableCommand {
    QueryResultContext ctx;
    ComposableCommand parent;

    public PGCmdResponse(QueryResultContext ctx, ComposableCommand parent) {
        this.parent = parent;
        this.ctx = ctx;
    }

    @Override protected boolean doInnerExecute() throws QuitException {
        while (true) {
            byte[] packet = ctx.packets.poll();
            if (Objects.isNull(packet)) {
                return false;
            }
            byte type = PGMessage.getType(packet);
            switch (type) {
                case PGFlags.BIND_COMPLETE:
                case PGFlags.CLOSE_COMPLETE:
                case PGFlags.B_COPY_DATA:
                case PGFlags.B_COPY_DONE:
                case PGFlags.COPY_OUT_RESPONSE:
                case PGFlags.EMPTY_QUERY_RESPONSE:
                case PGFlags.FUNCTION_CALL_RESPONSE:
                case PGFlags.NOTIFICATION_RESPONSE:
                case PGFlags.NOTICE_RESPONSE:
                case PGFlags.NO_DATA:
                case PGFlags.PARAMETER_DESCRIPTION:
                case PGFlags.PARAMETER_STATUS:
                case PGFlags.PARSE_COMPLETE:
                case PGFlags.PORTAL_SUSPENDED:
                    ctx.writeBytes(packet);
                    break;
                case PGFlags.COPY_IN_RESPONSE:
                    ctx.writeBytes(packet);
                    return true;
                case PGFlags.READY_FOR_QUERY:
                    handleReadyForQuery(packet);
                    return true;
                case PGFlags.COMMAND_COMPLETE:
                    handleCommandComplete(packet);
                    break;
                case PGFlags.ERROR_RESPONSE:
                    handleError(packet);
                    break;
                case PGFlags.DATA_ROW:
                    ctx.addRow(packet);
                    break;
                case PGFlags.ROW_DESCRIPTION:
                    ctx.addRowDescription(packet);
                    ctx.setResultType(ResultType.RESULT_SET);
                    break;
                case PGFlags.COPY_BOTH_RESPONSE:
                    throw new QuitException(
                        "UnSupported response type COPY_BOTH_RESPONSE, packet " + Arrays
                            .toString(packet));
                default:
                    throw new QuitException(
                        "unknown response type " + type + ", packet " + Arrays.toString(packet));
            }
        }
    }

    private void handleReadyForQuery(byte[] packet) {
        if (!isRewriteReadyForQuery()) {
            ctx.addReadyForQuery(packet);
            return;
        }
        if (ctx.resultType == ResultType.ERR) {
            ctx.addReadyForQuery(ReadyForQuery.IN_FAILED_TX.toPacket());
        } else {
            ctx.addReadyForQuery(ReadyForQuery.IN_TX.toPacket());
        }
        //增加etrace tag 来观察ReadyForQuery包被改写的行为
        DalMultiMessageProducer producer = ctx.sqlCtx.sqlSessionContextUtil.getDalEtraceProducer();
        if (Objects.nonNull(producer)) {
            producer.addTag("readyForQuery", "rewrited");
        }
    }

    private void handleCommandComplete(byte[] packet) {
        ctx.addCommandComplete(packet);
        CommandComplete ok = CommandComplete.loadFromPacket(packet);
        setSuccessResultType(ok.getTag());
        long affectedRows = ok.getRows();
        if (ctx.sqlCtx != null && ctx.sqlCtx.curCmdQuery != null) {
            ctx.sqlCtx.curCmdQuery.affectedRows = affectedRows;
        }
    }

    private void handleError(byte[] packet) {
        ctx.writeBytes(packet);
        ctx.setResultType(ResultType.ERR);
        ErrorResponse err = ErrorResponse.loadFromPacket(packet);
        String sqlState = err.getSqlState();
        if (ctx.sqlCtx != null) {
            ResponseStatus status = new ResponseStatus(ResponseStatus.ResponseType.DBERR,
                ErrorCode.getPGDBERRBySqlState(sqlState), err.getMessage());
            ctx.sqlCtx.sqlSessionContextUtil.trySetResponseStatus(status);
        }
    }

    private void setSuccessResultType(String tag) {
        if (tag.startsWith("SELECT")) {
            ctx.setResultType(ResultType.RESULT_SET);
        } else if (tag.startsWith("INSERT") || tag.startsWith("UPDATE") || tag
            .startsWith("DELETE")) {
            ctx.setResultType(ResultType.OK);
        } else {
            ctx.setResultType(ResultType.Other);
        }
    }

    /**
     * 当Client发送过BEGIN(isAutoCommit4Client=false)且isInTransStatus=false的时候
     * 尝试重写DB返回的ReadyForQuery数据包。
     *
     * @return true 尝试重写RFQ数据包, false不重写原样写回DB响应的RFQ
     */
    private boolean isRewriteReadyForQuery() {
        return ctx.sqlCtx.transactionController.canOpenTransaction() && !ctx.sqlCtx
            .isInTransStatus();
    }
}
