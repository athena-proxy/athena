package me.ele.jarch.athena.pg.proto;

import java.util.LinkedList;
import java.util.List;

/**
 * @author shaoyang.qi
 */
public class Execute extends PGMessage {
    private String portal = "";
    private int returns = -1;

    @Override protected byte getTypeByte() {
        return PGFlags.C_EXECUTE;
    }

    @Override protected List<byte[]> getPayload() {
        List<byte[]> bytes = new LinkedList<>();
        bytes.add(PGProto.buildNullStr(portal));
        bytes.add(PGProto.buildInt32BE(returns));
        return bytes;
    }

    public Execute() {
    }

    public Execute(String portal, int returns) {
        this.portal = portal;
        this.returns = returns;
    }

    public String getPortal() {
        return portal;
    }

    public void setPortal(String portal) {
        this.portal = portal;
    }

    public int getReturns() {
        return returns;
    }

    public void setReturns(int returns) {
        this.returns = returns;
    }

    public static Execute loadFromPacket(byte[] packet) {
        PGProto proto = new PGProto(packet, 5);
        String portal = proto.readNullStr();
        int returns = proto.readInt32();
        return new Execute(portal, returns);
    }

    @Override public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Execute {portal=");
        builder.append(portal);
        builder.append(", returns=");
        builder.append(returns);
        builder.append("}");
        return builder.toString();
    }

}
