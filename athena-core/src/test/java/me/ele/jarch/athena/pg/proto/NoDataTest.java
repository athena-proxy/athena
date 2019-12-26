package me.ele.jarch.athena.pg.proto;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author shaoyang.qi
 */
public class NoDataTest {
    @Test public void testToPacket() {
        byte[] bytes = NoData.INSTANCE.toPacket();

        int expectedTotalLength = 1 + 4;
        assertEquals(bytes.length, expectedTotalLength);

        PGProto proto = new PGProto(bytes);
        byte expectedType = PGFlags.NO_DATA;
        assertEquals(proto.readByte(), expectedType);
        int expectedPayloadLength = expectedTotalLength - 1;
        assertEquals(proto.readInt32(), expectedPayloadLength);
    }
}
