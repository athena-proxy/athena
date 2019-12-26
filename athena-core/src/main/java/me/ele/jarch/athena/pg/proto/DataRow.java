package me.ele.jarch.athena.pg.proto;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

/**
 * @author shaoyang.qi
 */
public class DataRow extends PGMessage {
    private int colCount = 0;
    private PGCol[] cols = null;

    @Override protected byte getTypeByte() {
        return PGFlags.DATA_ROW;
    }

    @Override protected List<byte[]> getPayload() {
        List<byte[]> bytes = new LinkedList<>();
        bytes.add(PGProto.buildInt16BE(colCount));
        if (colCount <= 0) {
            return bytes;
        }
        for (PGCol pgcol : cols) {
            bytes.add(pgcol.toPacket());
        }
        return bytes;
    }

    public DataRow() {
    }

    public DataRow(PGCol[] cols) {
        setCols(cols);
    }

    public PGCol[] getCols() {
        return cols;
    }

    public void setCols(PGCol[] cols) {
        this.cols = cols;
        this.colCount = this.cols == null ? 0 : this.cols.length;
    }

    public int getColCount() {
        return colCount;
    }

    public static DataRow loadFromPacket(byte[] packet) {
        PGProto proto = new PGProto(packet, 5);
        int colCount = proto.readInt16();
        if (colCount <= 0) {
            return new DataRow();
        }
        PGCol[] cols = new PGCol[colCount];
        for (int i = 0; i < colCount; i++) {
            int colDataLen = proto.readInt32();
            if (colDataLen < 0) {
                cols[i] = new PGCol();
                continue;
            }
            cols[i] = new PGCol(proto.readBytes(colDataLen));
        }
        return new DataRow(cols);
    }

    @Override public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("DataRow {colCount=");
        builder.append(colCount);
        builder.append(", cols=");
        builder.append(Arrays.toString(cols));
        builder.append("}");
        return builder.toString();
    }

    public static class PGCol {
        private int dataLen = -1;
        private byte[] data = null;

        public PGCol() {
        }

        public PGCol(byte[] data) {
            setData(data);
        }

        public byte[] getData() {
            return data;
        }

        public void setData(byte[] data) {
            this.data = data;
            this.dataLen = this.data == null ? -1 : this.data.length;
        }

        public int getDataLen() {
            return dataLen;
        }

        private List<byte[]> getPayload() {
            List<byte[]> bytes = new LinkedList<>();
            bytes.add(PGProto.buildInt32BE(dataLen));
            if (dataLen > 0) {
                bytes.add(data);
            }
            return bytes;
        }

        public byte[] toPacket() {
            List<byte[]> payload = this.getPayload();
            return PGMessage.toPacket(payload);
        }

        @Override public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("PGCol {dataLen=");
            builder.append(dataLen);
            builder.append(", data=");
            builder
                .append(Objects.isNull(data) ? "NULL" : new String(data, StandardCharsets.UTF_8));
            builder.append("}");
            return builder.toString();
        }

    }

}
