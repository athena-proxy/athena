package me.ele.jarch.athena.pg.proto;

import org.testng.annotations.Test;

import java.util.Arrays;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Created by jinghao.wang on 16/11/24.
 */
public class ParameterStatusTest {
    private final byte[] packet =
        new byte[] {(byte) 0x53, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x11, (byte) 0x54,
            (byte) 0x69, (byte) 0x6d, (byte) 0x65, (byte) 0x5a, (byte) 0x6f, (byte) 0x6e,
            (byte) 0x65, (byte) 0x00, (byte) 0x55, (byte) 0x54, (byte) 0x43, (byte) 0x00};

    @Test public void testToPacket() {
        ParameterStatus parameterStatus = new ParameterStatus("TimeZone", "UTC");
        assertTrue(Arrays.equals(parameterStatus.toPacket(), packet));
    }

    @Test public void testLoadFromPacket() throws Exception {
        ParameterStatus parameterStatus = ParameterStatus.loadFromPacket(packet);
        String expectName = "TimeZone";
        assertEquals(parameterStatus.getParamName(), expectName);
        String expectValue = "UTC";
        assertEquals(parameterStatus.getParamValue(), expectValue);
    }
}
