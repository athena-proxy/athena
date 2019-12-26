package me.ele.jarch.athena.pg.proto;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author shaoyang.qi
 */
public class QueryTest {
    @Test public void testToPacket() {
        String sql = "select 1";
        Query query = new Query(sql);
        byte[] bytes = query.toPacket();

        int expectedTotalLength = 1 + 4 + sql.length() + 1;
        assertEquals(bytes.length, expectedTotalLength);

        PGProto proto = new PGProto(bytes);
        byte expectedType = PGFlags.C_QUERY;
        assertEquals(proto.readByte(), expectedType);
        int expectedPayloadLength = expectedTotalLength - 1;
        assertEquals(proto.readInt32(), expectedPayloadLength);
        assertEquals(proto.readNullStr(), sql);
    }

    @Test public void testLoadFromPacket() {
        String sql = "select 1";
        Query query = new Query(sql);
        byte[] bytes = query.toPacket();

        Query newQuery = Query.loadFromPacket(bytes);
        assertEquals(newQuery.toPacket(), bytes);
        assertEquals(newQuery.getQuery(), sql);
    }
}
