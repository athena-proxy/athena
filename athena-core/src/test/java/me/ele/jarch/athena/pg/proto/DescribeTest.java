package me.ele.jarch.athena.pg.proto;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author shaoyang.qi
 */
public class DescribeTest {
    @Test public void testToPacket() {
        byte describe = Describe.PORTAL;
        String statement = "py:0x1023d39b0";
        Describe desc = new Describe(describe, statement);
        byte[] bytes = desc.toPacket();

        int expectedTotalLength = 1 + 4 + 1 + statement.length() + 1;
        assertEquals(bytes.length, expectedTotalLength);

        PGProto proto = new PGProto(bytes);
        byte expectedType = PGFlags.C_DESCRIBE;
        assertEquals(proto.readByte(), expectedType);
        int expectedPayloadLength = expectedTotalLength - 1;
        assertEquals(proto.readInt32(), expectedPayloadLength);
        assertEquals(proto.readByte(), describe);
        assertEquals(proto.readNullStr(), statement);
    }

    @Test public void testLoadFromPacket() {
        byte describe = Describe.PREPARE_STATEMENT;
        String statement = "py:0x1023d39b0";
        Describe desc = new Describe(describe, statement);
        byte[] bytes = desc.toPacket();

        Describe newDesc = Describe.loadFromPacket(bytes);
        assertEquals(newDesc.toPacket(), bytes);
        assertEquals(newDesc.describe, describe);
        assertEquals(newDesc.getStatement(), statement);
    }
}
