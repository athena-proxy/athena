package me.ele.jarch.athena.sql;

import com.github.mpjct.jmpjct.mysql.proto.*;
import com.github.mpjct.jmpjct.util.ErrorCode;
import me.ele.jarch.athena.constant.Metrics;
import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.util.ResponseStatus;
import me.ele.jarch.athena.util.etrace.MetricFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CmdColCnt extends ComposableCommand {
    private static final Logger logger = LoggerFactory.getLogger(CmdColCnt.class);

    long sequenceId;
    byte[] colCntPacket;

    ComposableCommand parent;
    QueryResultContext ctx;

    public CmdColCnt(QueryResultContext ctx, ComposableCommand parent) {
        this.parent = parent;
        this.ctx = ctx;
    }

    private void handleCommitFailure() throws QuitException {
        MetricFactory.newCounter(Metrics.COMMIT_FAILURE).once();
        String ctxInfo = "";
        ctx.sendAndFlush2Client();
        if (ctx.sqlCtx != null) {
            ctxInfo = ctx.sqlCtx.toString();
        }
        logger.error("COMMIT or ROLLBACK failure!! " + ctxInfo);
        throw new QuitException("COMMIT or ROLLBACK failure!!");
    }

    @Override public boolean doInnerExecute() throws QuitException {
        colCntPacket = ctx.packets.poll();
        if (colCntPacket == null) {
            return false;
        }
        ctx.writeBytes(colCntPacket);
        switch (Packet.getType(colCntPacket)) {
            case Flags.OK:
                ctx.setResultType(ResultType.OK);
                OK ok = OK.loadFromPacket(colCntPacket);
                ctx.ok = ok;
                if (ctx.sqlCtx != null && ctx.sqlCtx.curCmdQuery != null) {
                    ctx.sqlCtx.curCmdQuery.affectedRows = ok.affectedRows;
                }
                if (ok.hasStatusFlag(Flags.SERVER_MORE_RESULTS_EXISTS)) {
                    parent.appendCmd(new CmdColCnt(ctx, parent));
                }
                return true;
            case Flags.ERR:
                if (ctx.isEndTrans()) {
                    handleCommitFailure();
                }
                ctx.setResultType(ResultType.ERR);
                ERR err = ERR.loadFromPacket(colCntPacket);
                ctx.err = err;
                long errorCode = err.errorCode;
                if (ctx.sqlCtx != null) {
                    ctx.sqlCtx.sqlSessionContextUtil.trySetResponseStatus(
                        new ResponseStatus(ResponseStatus.ResponseType.DBERR,
                            ErrorCode.getDBERRByErrorNo((int) errorCode), err.errorMessage));
                }
                if (EOF.loadFromPacket(colCntPacket)
                    .hasStatusFlag(Flags.SERVER_MORE_RESULTS_EXISTS)) {
                    parent.appendCmd(new CmdColCnt(ctx, parent));
                }
                return true;
        }
        ctx.setResultType(ResultType.RESULT_SET);
        sequenceId = Packet.getSequenceId(colCntPacket);

        // Assume we have the start of a result set already
        long colCount = ColCount.loadFromPacket(colCntPacket).colCount;

        // Read the columns and the EOF field
        for (int i = 0; i < colCount; i++) {
            parent.appendCmd(new CmdCol(ctx));
        }
        parent.appendCmd(new CmdEOF(ctx));
        parent.appendCmd(new CmdRows(ctx, parent));
        return true;
    }
}
