package me.ele.jarch.athena.sql;

import io.netty.buffer.ByteBuf;
import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.pg.proto.PGProto;
import me.ele.jarch.athena.util.PacketLengthUtil;

public class PGCmdTcpPacket extends CmdTcpPacket {

    public PGCmdTcpPacket(ByteBuf input_buf) {
        super(input_buf);
    }

    @Override public boolean doInnerExecute() throws QuitException {
        // wait until the heading size is prepared
        if (input_buf.readableBytes() < 5 && expectedSize == -1) {
            return false;
        }

        // read the heading size
        if (expectedSize == -1) {
            byte type = input_buf.readByte();
            expectedSize = input_buf.readInt();
            PacketLengthUtil.checkValidLength(expectedSize);
            packet = new byte[expectedSize + 1];
            packet[0] = type;
            System.arraycopy(PGProto.buildInt32BE(expectedSize), 0, packet, 1, 4);
        }

        // wait until whole payload prepared
        if (input_buf.readableBytes() < expectedSize - 4) {
            return false;
        }

        // read the whole packet
        input_buf.readBytes(packet, 5, expectedSize - 4);

        // reuse the buffer memory
        input_buf.discardSomeReadBytes();
        return true;
    }
}
