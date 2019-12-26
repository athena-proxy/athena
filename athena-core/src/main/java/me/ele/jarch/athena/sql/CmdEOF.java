package me.ele.jarch.athena.sql;

import me.ele.jarch.athena.exception.QuitException;

public class CmdEOF extends ComposableCommand {
    QueryResultContext ctx;

    public CmdEOF(QueryResultContext ctx) {
        this.ctx = ctx;
    }

    @Override public boolean doInnerExecute() throws QuitException {
        byte[] packet = ctx.packets.poll();
        if (packet == null) {
            return false;
        }
        ctx.writeBytes(packet);
        return true;
    }
}
