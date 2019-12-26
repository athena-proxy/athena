package me.ele.jarch.athena.sql;

import com.github.mpjct.jmpjct.mysql.proto.Handshake;

public class CmdHandshakeFake extends OnePacketCommand {

    public Handshake handshake;
    private final long connectionId;

    public CmdHandshakeFake(long connectionId) {
        super(new byte[0]);
        this.connectionId = connectionId;
    }

    @Override public boolean doInnerExecute() {
        Handshake handshake = new Handshake();
        handshake.pack(connectionId);
        this.handshake = handshake;
        sendBuf = handshake.toPacket();
        return true;
    }
}
