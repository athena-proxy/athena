package me.ele.jarch.athena.pg.proto;

import me.ele.jarch.athena.sql.QueryPacket;

import java.util.Collections;
import java.util.List;

/**
 * @author shaoyang.qi
 */
public class Query extends PGMessage implements QueryPacket {
    public static final byte[] BEGIN_BYTES = new Query("BEGIN").toPacket();

    private String query = "";

    public static byte[] getBeginBytes() {
        return BEGIN_BYTES;
    }

    @Override protected byte getTypeByte() {
        return PGFlags.C_QUERY;
    }

    @Override protected List<byte[]> getPayload() {
        return Collections.singletonList(PGProto.buildNullStr(query));
    }

    public Query() {
        // empty query
    }

    public Query(String query) {
        setQuery(query);
    }

    @Override public String getQuery() {
        return query;
    }

    @Override public void setQuery(String query) {
        this.query = query;
    }

    public static Query loadFromPacket(byte[] packet) {
        String query = new PGProto(packet, 5).readNullStr();
        return new Query(query);
    }

    @Override public String toString() {
        final StringBuilder sb = new StringBuilder("Query{");
        sb.append("query='").append(query).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
