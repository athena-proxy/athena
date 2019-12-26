package com.github.mpjct.jmpjct.mysql.proto;

import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertArrayEquals;

public class HandshakeResponseTest {

    @Test public void test_1() {
        byte[] packet = new byte[] {(byte) 0x2f, (byte) 0x00, (byte) 0x00, (byte) 0x01, (byte) 0x0d,
            (byte) 0xa6, (byte) 0x03, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
            (byte) 0x01, (byte) 0x08, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
            (byte) 0x00, (byte) 0x72, (byte) 0x6f, (byte) 0x6f, (byte) 0x74, (byte) 0x00,
            (byte) 0x00, (byte) 0x73, (byte) 0x79, (byte) 0x73, (byte) 0x62, (byte) 0x65,
            (byte) 0x6e, (byte) 0x63, (byte) 0x68, (byte) 0x00};
        assertArrayEquals(packet, HandshakeResponse.loadFromPacket(packet).toPacket());
    }

    @Test public void test_5_5_8() {
        // https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse
        byte[] packet = new byte[] {(byte) 0x54, (byte) 0x00, (byte) 0x00, (byte) 0x01, (byte) 0x8d,
            (byte) 0xa6, (byte) 0x0f, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
            (byte) 0x01, (byte) 0x08, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
            (byte) 0x00, (byte) 0x70, (byte) 0x61, (byte) 0x6d, (byte) 0x00, (byte) 0x14,
            (byte) 0xab, (byte) 0x09, (byte) 0xee, (byte) 0xf6, (byte) 0xbc, (byte) 0xb1,
            (byte) 0x32, (byte) 0x3e, (byte) 0x61, (byte) 0x14, (byte) 0x38, (byte) 0x65,
            (byte) 0xc0, (byte) 0x99, (byte) 0x1d, (byte) 0x95, (byte) 0x7d, (byte) 0x75,
            (byte) 0xd4, (byte) 0x47, (byte) 0x74, (byte) 0x65, (byte) 0x73, (byte) 0x74,
            (byte) 0x00, (byte) 0x6d, (byte) 0x79, (byte) 0x73, (byte) 0x71, (byte) 0x6c,
            (byte) 0x5f, (byte) 0x6e, (byte) 0x61, (byte) 0x74, (byte) 0x69, (byte) 0x76,
            (byte) 0x65, (byte) 0x5f, (byte) 0x70, (byte) 0x61, (byte) 0x73, (byte) 0x73,
            (byte) 0x77, (byte) 0x6f, (byte) 0x72, (byte) 0x64, (byte) 0x00,};

        assertArrayEquals(packet, HandshakeResponse.loadFromPacket(packet).toPacket());
    }

    @Test public void test_3_2_0() {
        // https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse
        byte[] packet = new byte[] {(byte) 0x11, (byte) 0x00, (byte) 0x00, (byte) 0x01, (byte) 0x85,
            (byte) 0x24, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x6f, (byte) 0x6c,
            (byte) 0x64, (byte) 0x00, (byte) 0x47, (byte) 0x44, (byte) 0x53, (byte) 0x43,
            (byte) 0x51, (byte) 0x59, (byte) 0x52, (byte) 0x5f};
        assertArrayEquals(packet, HandshakeResponse.loadFromPacket(packet).toPacket());
    }
}
