package com.github.mpjct.jmpjct.mysql.proto;

import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertArrayEquals;
import static org.testng.AssertJUnit.assertEquals;

public class AuthSwitchResponseTest {
    @Test public void test1() {
        byte[] packet =
            Proto.packet_string_to_bytes("" + "09 00 00 03 5c 49 4d 5e    4e 58 4f 47 00");

        AuthSwitchResponse pkt = AuthSwitchResponse.loadFromPacket(packet);
        assertArrayEquals(packet, pkt.toPacket());
        assertEquals(pkt.authPluginResponse, "XElNXk5YT0cA");
    }

    @Test public void test2() {
        byte[] packet = Proto.packet_string_to_bytes(
            "" + "14 00 00 03 f4 17 96 1f    79 f3 ac 10 0b da a6 b3" + "b5 c2 0e ab 59 85 ff b8");

        AuthSwitchResponse pkt = AuthSwitchResponse.loadFromPacket(packet);
        assertArrayEquals(packet, pkt.toPacket());
        assertEquals(pkt.authPluginResponse, "9BeWH3nzrBAL2qaztcIOq1mF/7g=");
    }
}
