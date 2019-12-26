package me.ele.jarch.athena.pg.proto;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author shaoyang.qi
 */
public class ExecuteTest {
    @Test public void testToPacket() {
        String portal = "py:0x1023d39b0";
        int returns = -1;
        Execute exec = new Execute(portal, returns);
        byte[] bytes = exec.toPacket();

        int expectedTotalLength = 1 + 4 + portal.length() + 1 + 4;
        assertEquals(bytes.length, expectedTotalLength);

        PGProto proto = new PGProto(bytes);
        byte expectedType = PGFlags.C_EXECUTE;
        assertEquals(proto.readByte(), expectedType);
        int expectedPayloadLength = expectedTotalLength - 1;
        assertEquals(proto.readInt32(), expectedPayloadLength);
        assertEquals(proto.readNullStr(), portal);
        assertEquals(proto.readInt32(), returns);
    }

    @Test public void testLoadFromPacket() {
        String portal = "py:0x1023d39b0";
        int returns = 0;
        Execute exec = new Execute(portal, returns);
        byte[] bytes = exec.toPacket();

        Execute newExec = Execute.loadFromPacket(bytes);
        assertEquals(newExec.toPacket(), bytes);
        assertEquals(newExec.getPortal(), portal);
        assertEquals(newExec.getReturns(), returns);
    }
}
