package me.ele.jarch.athena.pg.proto;

import org.testng.annotations.Test;

import java.util.Arrays;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Created by jinghao.wang on 16/11/24.
 */
public class BackendKeyDataTest {
    private final byte[] packet =
        new byte[] {(byte) 0x4b, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x0c, (byte) 0x00,
            (byte) 0x00, (byte) 0x00, (byte) 0x1f, (byte) 0x2f, (byte) 0x1a, (byte) 0x5a,
            (byte) 0xbf};

    @Test public void testToPacket() {
        BackendKeyData backendKeyData = new BackendKeyData(31, 790256319);
        assertTrue(Arrays.equals(backendKeyData.toPacket(), packet));
    }

    @Test public void testLoadFromPacket() throws Exception {
        BackendKeyData backendKeyData = BackendKeyData.loadFromPacket(packet);
        int expectedPid = 31;
        assertEquals(backendKeyData.getPid(), expectedPid);
        int expectedKey = 790256319;
        assertEquals(backendKeyData.getKey(), expectedKey);
    }
}
