package com.github.mpjct.jmpjct.mysql.proto;

import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertArrayEquals;
import static org.testng.AssertJUnit.assertEquals;

public class OKTest {
    @Test public void test1() {
        byte[] packet = Proto.packet_string_to_bytes("07 00 00 02 00 00 00 02    00 00 00");

        OK ok = OK.loadFromPacket(packet);
        assertArrayEquals(packet, ok.toPacket());
        assertEquals(ok.affectedRows, 0);
        assertEquals(ok.lastInsertId, 0);
        assertEquals(ok.hasStatusFlag(Flags.SERVER_STATUS_AUTOCOMMIT), true);
        assertEquals(ok.hasStatusFlag(Flags.SERVER_STATUS_IN_TRANS_READONLY), false);
        assertEquals(ok.warnings, 0);
    }
}
