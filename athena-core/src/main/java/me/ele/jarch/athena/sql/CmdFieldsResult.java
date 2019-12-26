package me.ele.jarch.athena.sql;

import com.github.mpjct.jmpjct.mysql.proto.Flags;
import com.github.mpjct.jmpjct.mysql.proto.Packet;
import me.ele.jarch.athena.exception.QuitException;

public class CmdFieldsResult extends ComposableCommand {
    QueryResultContext ctx;

    public CmdFieldsResult(QueryResultContext ctx) {
        this.ctx = ctx;
    }

    @Override public boolean doInnerExecute() throws QuitException {
        while (true) {
            byte[] colPacket = ctx.packets.poll();
            if (colPacket == null) {
                return false;
            }
            ctx.writeBytes(colPacket);
            switch (Packet.getType(colPacket)) {
                case Flags.OK:
                case Flags.EOF:
                case Flags.ERR:
                    return true;
            }
        }
    }

    public QueryResultContext getCtx() {
        return ctx;
    }
}
