package me.ele.jarch.athena.pg.proto;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author shaoyang.qi
 */
public class BindCompleteTest {
    @Test public void testToPacket() {
        byte[] bytes = BindComplete.INSTANCE.toPacket();

        int expectedTotalLength = 1 + 4;
        assertEquals(bytes.length, expectedTotalLength);

        PGProto proto = new PGProto(bytes);
        byte expectedType = PGFlags.BIND_COMPLETE;
        assertEquals(proto.readByte(), expectedType);
        int expectedPayloadLength = expectedTotalLength - 1;
        assertEquals(proto.readInt32(), expectedPayloadLength);
    }
}
