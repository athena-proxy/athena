package com.github.mpjct.jmpjct.mysql.proto;

import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertArrayEquals;
import static org.testng.AssertJUnit.assertEquals;

public class AuthMoreDataTest {
    @Test public void test1() {
        byte[] packet = Proto.packet_string_to_bytes(
            "" + "2c 00 00 02 01 6d 79 73    71 6c 5f 6e 61 74 69 76"
                + "65 5f 70 61 73 73 77 6f    72 64 00 7a 51 67 34 69"
                + "36 6f 4e 79 36 3d 72 48    4e 2f 3e 2d 62 29 41 00");

        AuthMoreData pkt = AuthMoreData.loadFromPacket(packet);
        assertArrayEquals(packet, pkt.toPacket());
        assertEquals(pkt.pluginData,
            "bXlzcWxfbmF0aXZlX3Bhc3N3b3JkAHpRZzRpNm9OeTY9ckhOLz4tYilBAA==");
    }
}
