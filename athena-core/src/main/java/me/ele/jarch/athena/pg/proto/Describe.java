package me.ele.jarch.athena.pg.proto;

import java.util.LinkedList;
import java.util.List;

/**
 * @author shaoyang.qi
 */
public class Describe extends PGMessage {
    /**
     * 描述一个预备语句
     */
    public static final byte PREPARE_STATEMENT = 'S';
    /**
     * 描述一个入口
     */
    public static final byte PORTAL = 'P';

    public final byte describe;
    protected String statement = "";

    @Override protected byte getTypeByte() {
        return PGFlags.C_DESCRIBE;
    }

    @Override protected List<byte[]> getPayload() {
        List<byte[]> bytes = new LinkedList<>();
        bytes.add(new byte[] {describe});
        bytes.add(PGProto.buildNullStr(statement));
        return bytes;
    }

    public Describe(byte describe) {
        this.describe = describe;
    }

    public Describe(byte describe, String statement) {
        this(describe);
        setStatement(statement);
    }

    public String getStatement() {
        return statement;
    }

    public void setStatement(String statement) {
        this.statement = statement;
    }

    public static Describe loadFromPacket(byte[] packet) {
        PGProto proto = new PGProto(packet, 5);
        byte describe = proto.readByte();
        String statement = proto.readNullStr();
        return new Describe(describe, statement);
    }

    @Override public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Describe {describe=");
        builder.append(describe);
        builder.append(", statement=");
        builder.append(statement);
        builder.append("}");
        return builder.toString();
    }

}
