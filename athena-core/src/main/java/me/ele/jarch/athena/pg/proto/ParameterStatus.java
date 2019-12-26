package me.ele.jarch.athena.pg.proto;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by jinghao.wang on 16/11/24.
 */
public class ParameterStatus extends PGMessage {
    private final String paramName;
    private final String paramValue;

    @Override protected List<byte[]> getPayload() {
        List<byte[]> payload = new LinkedList<>();
        payload.add(PGProto.buildNullStr(paramName));
        payload.add(PGProto.buildNullStr(paramValue));
        return payload;
    }

    @Override protected byte getTypeByte() {
        return PGFlags.PARAMETER_STATUS;
    }

    public ParameterStatus(String paramName, String paramValue) {
        this.paramName = paramName;
        this.paramValue = paramValue;
    }

    public String getParamName() {
        return paramName;
    }

    public String getParamValue() {
        return paramValue;
    }

    public static ParameterStatus loadFromPacket(byte[] packet) {
        PGProto proto = new PGProto(packet, 5);
        return new ParameterStatus(proto.readNullStr(), proto.readNullStr());
    }

    @Override public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("ParameterStatus {paramName=");
        builder.append(paramName);
        builder.append(", paramValue=");
        builder.append(paramValue);
        builder.append("}");
        return builder.toString();
    }

}
