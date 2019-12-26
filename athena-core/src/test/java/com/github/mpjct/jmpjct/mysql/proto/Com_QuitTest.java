package com.github.mpjct.jmpjct.mysql.proto;

import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertArrayEquals;

public class Com_QuitTest {
    @Test public void test1() {
        byte[] packet = Proto.packet_string_to_bytes("" + "01 00 00 00 01");

        Com_Quit pkt = Com_Quit.loadFromPacket(packet);
        assertArrayEquals(packet, pkt.toPacket());
    }

    @Test public void test2() {
        Com_Quit pkt = Com_Quit.loadFromPacket(Com_Quit.getQuitPacketBytes());
        assertArrayEquals(Com_Quit.getQuitPacketBytes(), pkt.toPacket());
    }
}
