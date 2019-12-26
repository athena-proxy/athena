package me.ele.jarch.athena.sql;

import com.github.mpjct.jmpjct.mysql.proto.EOF;
import com.github.mpjct.jmpjct.mysql.proto.ERR;
import com.github.mpjct.jmpjct.mysql.proto.Flags;
import com.github.mpjct.jmpjct.mysql.proto.Packet;
import com.github.mpjct.jmpjct.util.ErrorCode;
import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.util.ResponseStatus;

public class CmdRows extends ComposableCommand {
    QueryResultContext ctx;
    ComposableCommand parent;
    long affectedRows = 0;

    public CmdRows(QueryResultContext ctx, ComposableCommand parent) {
        this.ctx = ctx;
        this.parent = parent;
    }

    @Override public boolean doInnerExecute() throws QuitException {
        while (true) {
            byte[] packet = ctx.packets.poll();
            if (packet == null) {
                return false;
            }
            int packetType = Packet.getType(packet);
            if (packetType != Flags.ERR && packetType != Flags.EOF) {
                ctx.addRow(packet);
                affectedRows++;
            } else {
                ctx.writeBytes(packet);
            }
            //ctx.assertMaxResultSize();

            if (packetType == Flags.ERR) {
                ERR err = ERR.loadFromPacket(packet);
                ctx.err = err;
                long errorCode = err.errorCode;
                if (ctx.sqlCtx != null) {
                    ctx.sqlCtx.sqlSessionContextUtil.trySetResponseStatus(
                        new ResponseStatus(ResponseStatus.ResponseType.DBERR,
                            ErrorCode.getDBERRByErrorNo((int) errorCode), err.errorMessage));
                }
                return true;
            }

            if (packetType == Flags.EOF) {
                if (EOF.loadFromPacket(packet).hasStatusFlag(Flags.SERVER_MORE_RESULTS_EXISTS)) {
                    parent.appendCmd(new CmdColCnt(ctx, parent));
                }
                if (ctx.sqlCtx != null && ctx.sqlCtx.curCmdQuery != null) {
                    ctx.sqlCtx.curCmdQuery.affectedRows = affectedRows;
                }
                return true;
            }
        }
    }
}
