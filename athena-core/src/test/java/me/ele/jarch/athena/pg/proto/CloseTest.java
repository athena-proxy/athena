package me.ele.jarch.athena.pg.proto;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author shaoyang.qi
 */
public class CloseTest {
    @Test public void testToPacket() {
        byte describe = Close.PORTAL;
        String statement = "py:0x1023d39b0";
        Close close = new Close(describe, statement);
        byte[] bytes = close.toPacket();

        int expectedTotalLength = 1 + 4 + 1 + statement.length() + 1;
        assertEquals(bytes.length, expectedTotalLength);

        PGProto proto = new PGProto(bytes);
        byte expectedType = PGFlags.C_CLOSE;
        assertEquals(proto.readByte(), expectedType);
        int expectedPayloadLength = expectedTotalLength - 1;
        assertEquals(proto.readInt32(), expectedPayloadLength);
        assertEquals(proto.readByte(), describe);
        assertEquals(proto.readNullStr(), statement);
    }

    @Test public void testLoadFromPacket() {
        byte describe = Close.PREPARE_STATEMENT;
        String statement = "py:0x1023d39b0";
        Close close = new Close(describe, statement);
        byte[] bytes = close.toPacket();

        Close newClose = Close.loadFromPacket(bytes);
        assertEquals(newClose.toPacket(), bytes);
        assertEquals(newClose.describe, describe);
        assertEquals(newClose.getStatement(), statement);
    }
}
