package me.ele.jarch.athena.pg.proto;

/**
 * @author shaoyang.qi
 */
public class Close extends Describe {

    @Override protected byte getTypeByte() {
        return PGFlags.C_CLOSE;
    }

    public Close(byte describe) {
        super(describe);
    }

    public Close(byte describe, String statement) {
        super(describe, statement);
    }

    public static Close loadFromPacket(byte[] packet) {
        PGProto proto = new PGProto(packet, 5);
        byte describe = proto.readByte();
        String statement = proto.readNullStr();
        return new Close(describe, statement);
    }

    @Override public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Close {describe=");
        builder.append(describe);
        builder.append(", statement=");
        builder.append(statement);
        builder.append("}");
        return builder.toString();
    }

}
