package me.ele.jarch.athena.pg.proto;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * @author shaoyang.qi
 */
public class ParameterDescription extends PGMessage {
    private int paramCount = 0;
    private int[] paramIds = null;

    @Override protected byte getTypeByte() {
        return PGFlags.PARAMETER_DESCRIPTION;
    }

    @Override protected List<byte[]> getPayload() {
        List<byte[]> bytes = new LinkedList<>();
        bytes.add(PGProto.buildInt16BE(paramCount));
        if (paramCount <= 0) {
            return bytes;
        }
        for (int paramId : paramIds) {
            bytes.add(PGProto.buildInt32BE(paramId));
        }
        return bytes;
    }

    public ParameterDescription() {
    }

    public ParameterDescription(int[] paramIds) {
        setParamIds(paramIds);
    }

    public int[] getParamIds() {
        return paramIds;
    }

    public void setParamIds(int[] paramIds) {
        this.paramIds = paramIds;
        this.paramCount = this.paramIds == null ? 0 : this.paramIds.length;
    }

    public int getParamCount() {
        return paramCount;
    }

    public static ParameterDescription loadFromPacket(byte[] packet) {
        PGProto proto = new PGProto(packet, 5);
        int paramCount = proto.readInt16();
        if (paramCount <= 0) {
            return new ParameterDescription();
        }
        int[] paramIds = new int[paramCount];
        for (int i = 0; i < paramCount; i++) {
            paramIds[i] = proto.readInt32();
        }
        return new ParameterDescription(paramIds);
    }

    @Override public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("ParameterDescription {paramCount=");
        builder.append(paramCount);
        builder.append(", paramIds=");
        builder.append(Arrays.toString(paramIds));
        builder.append("}");
        return builder.toString();
    }

}
