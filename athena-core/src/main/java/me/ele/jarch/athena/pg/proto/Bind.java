package me.ele.jarch.athena.pg.proto;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * @author shaoyang.qi
 */
public class Bind extends PGMessage {
    private String portal = "";
    private String statement = "";
    private int paramFormatIdCount = 0;
    private int[] paramFormatIds = null;
    private int paramCount = 0;
    private Param[] params = null;
    private ResultFormat resultFormat = new ResultFormat();

    @Override protected byte getTypeByte() {
        return PGFlags.C_BIND;
    }

    @Override protected List<byte[]> getPayload() {
        List<byte[]> bytes = new LinkedList<byte[]>();
        bytes.add(PGProto.buildNullStr(portal));
        bytes.add(PGProto.buildNullStr(statement));
        bytes.add(PGProto.buildInt16BE(paramFormatIdCount));
        for (int i = 0; i < paramFormatIdCount; i++) {
            bytes.add(PGProto.buildInt16BE(paramFormatIds[i]));
        }
        bytes.add(PGProto.buildInt16BE(paramCount));
        for (int i = 0; i < paramCount; i++) {
            bytes.add(params[i].toPacket());
        }
        bytes.add(resultFormat.toPacket());
        return bytes;
    }

    public String getPortal() {
        return portal;
    }

    public void setPortal(String portal) {
        this.portal = portal;
    }

    public String getStatement() {
        return statement;
    }

    public void setStatement(String statement) {
        this.statement = statement;
    }

    public int[] getParamFormatIds() {
        return paramFormatIds;
    }

    public void setParamFormatIds(int[] paramFormatIds) {
        this.paramFormatIds = paramFormatIds;
        this.paramFormatIdCount = this.paramFormatIds == null ? 0 : this.paramFormatIds.length;
    }

    public int getParamFormatIdCount() {
        return paramFormatIdCount;
    }

    public Param[] getParams() {
        return params;
    }

    public void setParams(Param[] params) {
        this.params = params;
        this.paramCount = this.params == null ? 0 : this.params.length;
    }

    public int getParamCount() {
        return paramCount;
    }

    public ResultFormat getResultFormat() {
        return resultFormat;
    }

    public void setResultFormat(ResultFormat resultFormat) {
        this.resultFormat = resultFormat;
    }

    public static Bind loadFromPacket(byte[] packet) {
        PGProto proto = new PGProto(packet, 5);
        Bind bind = new Bind();
        bind.setPortal(proto.readNullStr());
        bind.setStatement(proto.readNullStr());
        int paramFormatIdCount = proto.readInt16();
        int[] paramFormatIds = new int[paramFormatIdCount];
        for (int i = 0; i < paramFormatIdCount; i++) {
            paramFormatIds[i] = proto.readInt16();
        }
        bind.setParamFormatIds(paramFormatIds);
        int paramCount = proto.readInt16();
        Param[] params = new Param[paramCount];
        for (int i = 0; i < paramCount; i++) {
            Param param = new Param();
            int paramLen = proto.readInt32();
            if (paramLen >= 0) {
                param.setParamVals(proto.readBytes(paramLen));
            }
            params[i] = param;
        }
        bind.setParams(params);
        int resultFormatCount = proto.readInt16();
        int[] resultFormatVals = new int[resultFormatCount];
        for (int i = 0; i < resultFormatCount; i++) {
            resultFormatVals[i] = proto.readInt16();
        }
        bind.setResultFormat(new ResultFormat(resultFormatVals));
        return bind;
    }

    @Override public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Bind {portal=");
        builder.append(portal);
        builder.append(", statement=");
        builder.append(statement);
        builder.append(", paramFormatIdCount=");
        builder.append(paramFormatIdCount);
        builder.append(", paramFormatIds=");
        builder.append(Arrays.toString(paramFormatIds));
        builder.append(", paramCount=");
        builder.append(paramCount);
        builder.append(", params=");
        builder.append(Arrays.toString(params));
        builder.append(", resultFormat=");
        builder.append(resultFormat);
        builder.append("}");
        return builder.toString();
    }

    public static class Param {
        private int paramLen = -1;
        private byte[] paramVals = null;

        public Param() {
        }

        public Param(byte[] paramVals) {
            setParamVals(paramVals);
        }

        public byte[] getParamVals() {
            return paramVals;
        }

        public void setParamVals(byte[] paramVals) {
            this.paramVals = paramVals;
            this.paramLen = this.paramVals == null ? -1 : this.paramVals.length;
        }

        public int getParamLen() {
            return paramLen;
        }

        private List<byte[]> getPayload() {
            List<byte[]> bytes = new LinkedList<>();
            bytes.add(PGProto.buildInt32BE(paramLen));
            if (paramLen <= 0) {
                return bytes;
            }
            bytes.add(paramVals);
            return bytes;
        }

        public byte[] toPacket() {
            List<byte[]> payload = this.getPayload();
            return PGMessage.toPacket(payload);
        }

        @Override public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("Param {paramLen=");
            builder.append(paramLen);
            builder.append(", paramVals=");
            builder.append(Arrays.toString(paramVals));
            builder.append("}");
            return builder.toString();
        }

    }


    public static class ResultFormat {
        private int resultFormatCount = 0;
        private int[] resultFormatVals = new int[0];

        public ResultFormat() {
        }

        public ResultFormat(int[] resultFormatVals) {
            setResultFormatVals(resultFormatVals);
        }

        public int[] getResultFormatVals() {
            return resultFormatVals;
        }

        public void setResultFormatVals(int[] resultFormatVals) {
            this.resultFormatVals = resultFormatVals;
            this.resultFormatCount =
                this.resultFormatVals == null ? 0 : this.resultFormatVals.length;
        }

        public int getResultFormatCount() {
            return resultFormatCount;
        }

        private List<byte[]> getPayload() {
            List<byte[]> bytes = new LinkedList<>();
            bytes.add(PGProto.buildInt16BE(resultFormatCount));
            for (int i = 0; i < resultFormatCount; i++) {
                bytes.add(PGProto.buildInt16BE(resultFormatVals[i]));
            }
            return bytes;
        }

        public byte[] toPacket() {
            List<byte[]> payload = this.getPayload();
            return PGMessage.toPacket(payload);
        }

        @Override public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("ResultFormat {resultFormatCount=");
            builder.append(resultFormatCount);
            builder.append(", resultFormatVals=");
            builder.append(Arrays.toString(resultFormatVals));
            builder.append("}");
            return builder.toString();
        }

    }
}
