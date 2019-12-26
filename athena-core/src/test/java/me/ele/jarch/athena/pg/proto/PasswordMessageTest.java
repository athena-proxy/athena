package me.ele.jarch.athena.pg.proto;

import org.testng.annotations.Test;

import java.util.Arrays;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Created by jinghao.wang on 16/11/24.
 */
public class PasswordMessageTest {
    private final byte[] packet =
        new byte[] {(byte) 0x70, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x28, (byte) 0x6d,
            (byte) 0x64, (byte) 0x35, (byte) 0x32, (byte) 0x62, (byte) 0x66, (byte) 0x33,
            (byte) 0x32, (byte) 0x30, (byte) 0x62, (byte) 0x32, (byte) 0x66, (byte) 0x34,
            (byte) 0x64, (byte) 0x35, (byte) 0x37, (byte) 0x32, (byte) 0x34, (byte) 0x62,
            (byte) 0x38, (byte) 0x38, (byte) 0x37, (byte) 0x65, (byte) 0x66, (byte) 0x62,
            (byte) 0x37, (byte) 0x30, (byte) 0x63, (byte) 0x63, (byte) 0x65, (byte) 0x64,
            (byte) 0x61, (byte) 0x65, (byte) 0x38, (byte) 0x66, (byte) 0x00};
    private final byte[] salt = new byte[] {(byte) 0x45, (byte) 0xea, (byte) 0x85, (byte) 0x0f};

    @Test public void testToPacket() throws Exception {
        PasswordMessage passwordMessage = new PasswordMessage("root", "root", salt);
        assertTrue(Arrays.equals(passwordMessage.toPacket(), packet));

        String expectedPassword = "md52bf320b2f4d5724b887efb70ccedae8f";
        assertEquals(passwordMessage.getPassword(), expectedPassword);
    }

    @Test public void testLoadFromPacket() {
        PasswordMessage passwordMessage = PasswordMessage.loadFromPacket(packet);
        String expectedPassword = "md52bf320b2f4d5724b887efb70ccedae8f";
        assertEquals(passwordMessage.getPassword(), expectedPassword);
    }
}
