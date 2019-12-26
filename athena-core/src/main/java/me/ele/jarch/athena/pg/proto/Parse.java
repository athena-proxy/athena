package me.ele.jarch.athena.pg.proto;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * @author shaoyang.qi
 */
public class Parse extends PGMessage {
    private String statement = "";
    private String query = "";
    private int paramOidCount = 0;
    private int[] paramOids = null;

    @Override protected byte getTypeByte() {
        return PGFlags.C_PARSE;
    }

    @Override protected List<byte[]> getPayload() {
        List<byte[]> bytes = new LinkedList<byte[]>();
        bytes.add(PGProto.buildNullStr(statement));
        bytes.add(PGProto.buildNullStr(query));
        bytes.add(PGProto.buildInt16BE(paramOidCount));
        for (int i = 0; i < paramOidCount; i++) {
            bytes.add(PGProto.buildInt32BE(paramOids[i]));
        }
        return bytes;
    }

    public String getStatement() {
        return statement;
    }

    public void setStatement(String statement) {
        this.statement = statement;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public int[] getParamOids() {
        return paramOids;
    }

    public void setParamOids(int[] paramOids) {
        this.paramOids = paramOids;
        this.paramOidCount = this.paramOids == null ? 0 : this.paramOids.length;
    }

    public int getParamOidCount() {
        return paramOidCount;
    }

    public static Parse loadFromPacket(byte[] packet) {
        PGProto proto = new PGProto(packet, 5);
        Parse parse = new Parse();
        parse.setStatement(proto.readNullStr());
        parse.setQuery(proto.readNullStr());
        int paramOidCount = proto.readInt16();
        int[] paramOids = new int[paramOidCount];
        for (int i = 0; i < paramOidCount; i++) {
            paramOids[i] = proto.readInt32();
        }
        parse.setParamOids(paramOids);
        return parse;
    }

    @Override public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Parse {statement=");
        builder.append(statement);
        builder.append(", query=");
        builder.append(query);
        builder.append(", paramOidCount=");
        builder.append(paramOidCount);
        builder.append(", paramOids=");
        builder.append(Arrays.toString(paramOids));
        builder.append("}");
        return builder.toString();
    }

}
