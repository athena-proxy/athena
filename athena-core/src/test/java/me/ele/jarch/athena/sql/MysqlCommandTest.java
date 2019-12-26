package me.ele.jarch.athena.sql;

import com.github.mpjct.jmpjct.mysql.proto.Handshake;
import com.github.mpjct.jmpjct.mysql.proto.Packet;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import me.ele.jarch.athena.exception.QuitException;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class MysqlCommandTest {

    @Test public void testCommandExecute() throws QuitException {
        ComposableCommand cmd = new ComposableCommand() {

            @Override public boolean doInnerExecute() {
                return true;
            }
        };
        cmd.execute();
        assertTrue(cmd.isDone());
    }

    @Test public void testCmdTcpPacket() throws QuitException {
        assertEquals(Packet.getSize(new byte[] {0x1, 0x2, 0x0}), 513);
        ByteBuf buf = Unpooled.buffer();
        CmdTcpPacket cmd = new MySQLCmdTcpPacket(buf);

        buf.writeBytes(new byte[] {0x1});
        cmd.execute();
        assertFalse(cmd.isDone());

        buf.writeBytes(new byte[] {0x2});
        cmd.execute();
        assertFalse(cmd.isDone());

        byte[] b = new byte[1 + 1 + 413];// 0x0 + sequenceId + 413 bytes
        final int sequenceId = 100;
        b[0] = 0x0;
        b[1] = sequenceId;
        buf.writeBytes(b);
        cmd.execute();
        assertFalse(cmd.isDone());

        // write remaining 100 bytes
        buf.writeBytes(new byte[100]);
        cmd.execute();
        assertTrue(cmd.isDone());
        assertEquals(sequenceId, Handshake.loadFromPacket(cmd.packet).sequenceId);
    }

    @Test public void testCmdTcpPacketMorePacket() throws QuitException {
        assertEquals(Packet.getSize(new byte[] {0x1, 0x2, 0x0}), 513);
        ByteBuf buf = Unpooled.buffer();
        CmdTcpPacket cmd = new MySQLCmdTcpPacket(buf);

        buf.writeBytes(new byte[] {0x1});
        cmd.execute();
        assertFalse(cmd.isDone());

        buf.writeBytes(new byte[] {0x2});
        cmd.execute();
        assertFalse(cmd.isDone());

        byte[] b = new byte[1 + 1 + 513 + 50];// 0x0 + sequenceId + 513 bytes+50 more bytes
        b[0] = 0x0;
        buf.writeBytes(b);
        cmd.execute();
        assertTrue(cmd.isDone());
        assertEquals(50, buf.readableBytes());
    }
}
