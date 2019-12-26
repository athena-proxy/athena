package com.github.mpjct.jmpjct.mysql.proto;

import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertArrayEquals;
import static org.testng.AssertJUnit.assertEquals;

public class EOFTest {
    @Test public void test1() {
        byte[] packet = Proto.packet_string_to_bytes("05 00 00 05 fe 00 00 02 00");

        EOF pkt = EOF.loadFromPacket(packet);
        assertArrayEquals(packet, pkt.toPacket());
        assertEquals(pkt.warnings, 0);
        assertEquals(pkt.hasStatusFlag(Flags.SERVER_STATUS_AUTOCOMMIT), true);
        assertEquals(pkt.hasStatusFlag(Flags.SERVER_QUERY_WAS_SLOW), false);
    }
}
