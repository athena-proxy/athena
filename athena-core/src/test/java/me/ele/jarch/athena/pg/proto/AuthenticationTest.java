package me.ele.jarch.athena.pg.proto;

import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;

/**
 * Created by jinghao.wang on 16/11/25.
 */
public class AuthenticationTest {
    private final byte[] okPacket =
        new byte[] {(byte) 0x52, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x08, (byte) 0x00,
            (byte) 0x00, (byte) 0x00, (byte) 0x00};
    private final byte[] md5Packet = new AuthenticationMD5Password().toPacket();

    @Test public void testLoadFromPacket() throws Exception {
        Authentication md5Auth = Authentication.loadFromPacket(md5Packet);
        assertTrue(md5Auth instanceof AuthenticationMD5Password);

        Authentication okAuth = Authentication.loadFromPacket(okPacket);
        assertTrue(okAuth instanceof AuthenticationOk);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "not implement auth method.*")
    public void testLoadFromPacketException() {
        byte[] errorPacket =
            new byte[] {(byte) 0x52, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x08,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x02};
        Authentication.loadFromPacket(errorPacket);
    }
}
