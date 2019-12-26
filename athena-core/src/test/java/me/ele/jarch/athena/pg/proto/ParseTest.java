package me.ele.jarch.athena.pg.proto;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author shaoyang.qi
 */
public class ParseTest {
    @Test public void testToPacket() {
        String statement = "py:0x1023d39b0";
        String query = "insert into sharding_none (id, name) values ($1, $2)";
        int[] paramOids = new int[] {10};
        Parse parse = new Parse();
        parse.setStatement(statement);
        parse.setQuery(query);
        parse.setParamOids(paramOids);

        byte[] bytes = parse.toPacket();

        int expectedTotalLength =
            1 + 4 + statement.length() + 1 + query.length() + 1 + 2 + 4 * paramOids.length;
        assertEquals(bytes.length, expectedTotalLength);

        PGProto proto = new PGProto(bytes);
        byte expectedType = PGFlags.C_PARSE;
        assertEquals(proto.readByte(), expectedType);
        int expectedPayloadLength = expectedTotalLength - 1;
        assertEquals(proto.readInt32(), expectedPayloadLength);
        assertEquals(proto.readNullStr(), statement);
        assertEquals(proto.readNullStr(), query);
        assertEquals(proto.readInt16(), paramOids.length);
        assertEquals(proto.readInt32(), paramOids[0]);
    }

    @Test public void testLoadFromPacket() {
        String statement = "py:0x1023d39b0";
        String query = "insert into sharding_none (id, name) values ($1, $2)";
        int[] paramOids = new int[] {10};
        Parse parse = new Parse();
        parse.setStatement(statement);
        parse.setQuery(query);
        parse.setParamOids(paramOids);
        byte[] bytes = parse.toPacket();

        Parse newParse = Parse.loadFromPacket(bytes);
        assertEquals(newParse.toPacket(), bytes);
        assertEquals(newParse.getStatement(), statement);
        assertEquals(newParse.getQuery(), query);
        assertEquals(newParse.getParamOidCount(), paramOids.length);
        assertEquals(newParse.getParamOids(), paramOids);
    }
}
