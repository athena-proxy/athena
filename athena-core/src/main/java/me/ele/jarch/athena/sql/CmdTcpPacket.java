package me.ele.jarch.athena.sql;

import io.netty.buffer.ByteBuf;

public abstract class CmdTcpPacket extends ComposableCommand {
    protected ByteBuf input_buf;

    protected int expectedSize = -1;
    protected byte[] packet;

    public CmdTcpPacket(ByteBuf input_buf) {
        this.input_buf = input_buf;
    }

    public byte[] getPacket() {
        if (isDone()) {
            return this.packet;
        }
        return null;
    }

}
