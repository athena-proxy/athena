package me.ele.jarch.athena.pg.proto;

import org.testng.annotations.Test;

import java.util.Arrays;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Created by jinghao.wang on 16/11/24.
 */
public class ReadyForQueryTest {

    private final byte[] packet =
        new byte[] {(byte) 0x5a, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x05, (byte) 0x49};

    @Test public void TestToPacket() {
        ReadyForQuery readyForQuery = new ReadyForQuery((byte) 'I');
        assertTrue(Arrays.equals(readyForQuery.toPacket(), packet));
    }

    @Test public void testLoadFromPacket() throws Exception {
        ReadyForQuery readyForQuery = ReadyForQuery.loadFromPacket(packet);
        byte expectedStatus = (byte) 'I';
        assertEquals(readyForQuery.getStatus(), expectedStatus);
    }
}
