package me.ele.jarch.athena.sql;

import com.github.mpjct.jmpjct.mysql.proto.Packet;
import io.netty.buffer.ByteBuf;

public class MySQLCmdTcpPacket extends CmdTcpPacket {

    public MySQLCmdTcpPacket(ByteBuf input_buf) {
        super(input_buf);
    }

    @Override public boolean doInnerExecute() {
        // wait until the heading size is prepared
        if (input_buf.readableBytes() < 3 && expectedSize == -1) {
            return false;
        }

        // read the heading size
        if (expectedSize == -1) {
            byte[] sizeBuf = new byte[3];
            input_buf.readBytes(sizeBuf);
            expectedSize = Packet.getSize(sizeBuf);

            // alloc packet byte[] and fill first 3 bytes
            packet = new byte[expectedSize + 4];
            System.arraycopy(sizeBuf, 0, packet, 0, 3);
        }

        // wait until whole packet is prepared (packet size + sequenceId size)
        if (input_buf.readableBytes() < expectedSize + 1) {
            return false;
        }

        // read the whole packet
        input_buf.readBytes(packet, 3, expectedSize + 1);

        // reuse the buffer memory
        input_buf.discardSomeReadBytes();
        return true;
    }
}
