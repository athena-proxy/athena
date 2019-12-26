package com.github.mpjct.jmpjct.mysql.proto;

import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertArrayEquals;

public class OldAuthSwitchRequestTest {
    @Test public void test1() {
        byte[] packet = Proto.packet_string_to_bytes("" + "01 00 00 02 fe");

        OldAuthSwitchRequest pkt = OldAuthSwitchRequest.loadFromPacket(packet);
        assertArrayEquals(packet, pkt.toPacket());
    }
}
