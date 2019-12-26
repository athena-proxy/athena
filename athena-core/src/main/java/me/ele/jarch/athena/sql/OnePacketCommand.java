package me.ele.jarch.athena.sql;

public abstract class OnePacketCommand extends ComposableCommand {
    protected byte[] packet;
    protected byte[] sendBuf;

    public byte[] getSendBuf() {
        return sendBuf;
    }

    public OnePacketCommand(byte[] packet) {
        super();
        this.packet = packet;
    }
}
