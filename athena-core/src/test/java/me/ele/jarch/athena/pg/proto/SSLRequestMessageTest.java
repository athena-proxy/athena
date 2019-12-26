package me.ele.jarch.athena.pg.proto;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * Created by jinghao.wang on 16/11/25.
 */
public class SSLRequestMessageTest {
    private final byte[] packet =
        new byte[] {(byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x08, (byte) 0x04, (byte) 0xd2,
            (byte) 0x16, (byte) 0x2f};

    @Test public void testToPacket() {
        assertEquals(SSLRequestMessage.SSL_REQUEST_MESSAGE.toPacket(), packet);
    }
}
