package me.ele.jarch.athena.sql;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import me.ele.jarch.athena.pg.proto.AuthenticationOk;
import me.ele.jarch.athena.util.NoThrow;
import me.ele.jarch.athena.util.PacketLengthUtil;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

/**
 * Created by jie on 16/11/25.
 */
public class PGCmdTcpPacketTest {
    ByteBuf buf;
    PGCmdTcpPacket packet;

    @BeforeMethod public void setUp() throws Exception {
        buf = Unpooled.buffer();
        packet = new PGCmdTcpPacket(buf);
    }

    @Test public void testAuthOK() throws Exception {
        packet.execute();
        assertFalse(packet.isDone());
        buf.writeBytes(AuthenticationOk.INSTANCE.toPacket());
        packet.execute();
        assertTrue(packet.isDone());
        assertEquals(packet.getPacket(), AuthenticationOk.INSTANCE.toPacket());
        assertEquals(buf.readableBytes(), 0);
    }

    @Test public void testPartialPacket() throws Exception {
        byte[] auth_ok_type = new byte[] {(byte) 0x52};
        byte[] auth_ok_length = new byte[] {(byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x08};
        byte[] auth_ok_payload = new byte[] {(byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00};
        buf.writeBytes(auth_ok_type);
        packet.execute();
        assertFalse(packet.isDone());
        buf.writeBytes(auth_ok_length);
        packet.execute();
        assertFalse(packet.isDone());
        buf.writeBytes(auth_ok_payload);
        packet.execute();
        assertTrue(packet.isDone());
        assertEquals(buf.readableBytes(), 0);
        buf.writeBytes("more data".getBytes());
        packet.execute();
        assertEquals(buf.readableBytes(), "more data".getBytes().length);
    }

    @Test public void testPacketTooLarge() throws Exception {
        byte[] bytes = "dsarewrafs".getBytes();
        ByteBuf buf = Unpooled.wrappedBuffer(bytes);
        PGCmdTcpPacket packet = new PGCmdTcpPacket(buf);
        NoThrow.execute(() -> packet.execute(),
            (e) -> Assert.assertTrue(PacketLengthUtil.isPacketTooLargeException(e)));
    }
}
