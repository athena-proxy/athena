package me.ele.jarch.athena.pg.proto;

import org.testng.annotations.Test;

import java.util.Arrays;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Created by jinghao.wang on 16/11/24.
 */
public class AuthenticationOkTest {
    private final byte[] packet =
        new byte[] {(byte) 0x52, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x08, (byte) 0x00,
            (byte) 0x00, (byte) 0x00, (byte) 0x00};

    @Test public void testToPacket() {
        AuthenticationOk ok = new AuthenticationOk();
        assertTrue(Arrays.equals(ok.toPacket(), packet));
    }

    @Test public void testLoadFromPacket() {
        AuthenticationOk ok = AuthenticationOk.loadFromPacket(packet);
        assertEquals(ok.getTypeByte(), PGFlags.AUTHENTICATION_OK);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "illegal auth type byte.*")
    public void testLoadFromPacketException() {
        byte[] errorPacket =
            new byte[] {(byte) 0x52, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x08,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x02};
        AuthenticationOk ok = AuthenticationOk.loadFromPacket(errorPacket);
        assertEquals(ok.getTypeByte(), PGFlags.AUTHENTICATION_OK);
    }
}
