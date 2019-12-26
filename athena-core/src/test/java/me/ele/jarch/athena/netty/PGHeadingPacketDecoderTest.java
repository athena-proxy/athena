package me.ele.jarch.athena.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import me.ele.jarch.athena.pg.proto.*;
import me.ele.jarch.athena.util.NoThrow;
import me.ele.jarch.athena.util.PacketLengthUtil;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

/**
 * Created by jie on 16/11/25.
 */
public class PGHeadingPacketDecoderTest {
    PGHeadingPacketDecoder handler;
    EmbeddedChannel channel;

    @BeforeMethod public void setUp() throws Exception {
        handler = new PGHeadingPacketDecoder();
        channel = new EmbeddedChannel(handler);
    }

    @Test public void testDecodeSSLAndStartupMessage() throws Exception {
        // the handler has not been removed
        assertSame(channel.pipeline().first(), handler);
        // write half ssl request packet
        byte[] lengthByte = PGProto.buildInt32BE(SSLRequestMessage.SSL_REQUEST_LENGTH);
        channel.writeInbound(Unpooled.buffer().writeByte(lengthByte[0]));
        assertNull(channel.readInbound());
        channel.writeInbound(Unpooled.buffer().writeBytes(lengthByte, 1, 3));
        assertNull(channel.readInbound());
        // now write the whole ssl request packet
        channel.writeInbound(Unpooled.buffer().writeInt(SSLRequestMessage.SSL_REQUEST_NUMBER));
        ByteBuf buf = (ByteBuf) channel.readInbound();
        // the first byte is the virtual packet of ssl request
        assertEquals(buf.readByte(), PGFlags.C_SSL_REQUEST);
        // the rest packet length is 8 for ssl request
        assertEquals(8, buf.readableBytes());

        StartupMessage sm = new StartupMessage("root", "db");
        channel.writeInbound(Unpooled.wrappedBuffer(sm.toPacket()));
        buf = (ByteBuf) channel.readInbound();
        // the first byte is the virtual packet of startup
        assertEquals(buf.readByte(), PGFlags.C_STARTUP_MESSAGE);

        // the rest packet length is readl length of the startup
        assertEquals(buf.readableBytes(), sm.toPacket().length);

        // the handler has been removed
        assertNotSame(channel.pipeline().first(), handler);
    }

    @Test public void testDecodeOnlyStartupMessage() throws Exception {
        StartupMessage sm = new StartupMessage("root", "db");
        channel.writeInbound(Unpooled.wrappedBuffer(sm.toPacket()));
        ByteBuf buf = (ByteBuf) channel.readInbound();
        // the first byte is the virtual packet of startup
        assertEquals(buf.readByte(), PGFlags.C_STARTUP_MESSAGE);

        // the rest packet length is readl length of the startup
        assertEquals(buf.readableBytes(), sm.toPacket().length);

        // the handler has been removed
        assertNotSame(channel.pipeline().first(), handler);
    }

    @Test public void testCancelRequest() throws Exception {
        CancelRequest cr = new CancelRequest(111, 222);
        channel.writeInbound(Unpooled.wrappedBuffer(cr.toPacket()));
        ByteBuf buf = (ByteBuf) channel.readInbound();
        assertEquals(buf.readByte(), PGFlags.C_CANCEL_REQUEST);

        assertEquals(buf.readableBytes(), cr.toPacket().length);
        assertEquals(buf.readInt(), cr.toPacket().length);
        assertEquals(buf.readInt(), CancelRequest.CANCEL_REQUEST_CODE);
        assertEquals(buf.readInt(), 111);
        assertEquals(buf.readInt(), 222);
    }

    @Test public void testPacketTooLarge() throws Exception {
        byte[] bytes = "dsarewrw".getBytes();
        NoThrow.execute(() -> channel.writeInbound(Unpooled.wrappedBuffer(bytes)),
            (e) -> Assert.assertTrue(PacketLengthUtil.isPacketTooLargeException(e)));
    }
}
