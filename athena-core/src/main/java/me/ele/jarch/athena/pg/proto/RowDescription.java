package me.ele.jarch.athena.pg.proto;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * @author shaoyang.qi
 */
public class RowDescription extends PGMessage {
    private int colCount = 0;
    private PGColumn[] columns = null;

    @Override protected byte getTypeByte() {
        return PGFlags.ROW_DESCRIPTION;
    }

    @Override protected List<byte[]> getPayload() {
        List<byte[]> bytes = new LinkedList<>();
        bytes.add(PGProto.buildInt16BE(colCount));
        if (colCount <= 0) {
            return bytes;
        }
        for (PGColumn column : columns) {
            bytes.add(column.toPacket());
        }
        return bytes;
    }

    public RowDescription() {
    }

    public RowDescription(PGColumn[] columns) {
        setColumns(columns);
    }

    public PGColumn[] getColumns() {
        return columns;
    }

    public void setColumns(PGColumn[] columns) {
        this.columns = columns;
        this.colCount = this.columns == null ? 0 : this.columns.length;
    }

    public int getColCount() {
        return colCount;
    }

    public static RowDescription loadFromPacket(byte[] packet) {
        PGProto proto = new PGProto(packet, 5);
        int colCount = proto.readInt16();
        if (colCount <= 0) {
            return new RowDescription();
        }
        PGColumn[] columns = new PGColumn[colCount];
        for (int i = 0; i < colCount; i++) {
            PGColumn column = new PGColumn();
            column.setName(proto.readNullStr());
            column.setTableOid(proto.readInt32());
            column.setColNo(proto.readInt16());
            column.setTypeOid(proto.readInt32());
            column.setTypeLen(proto.readInt16());
            column.setTypeMod(proto.readInt32());
            column.setFormat(proto.readInt16());
            columns[i] = column;
        }
        return new RowDescription(columns);
    }

    @Override public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("RowDescription {colCount=");
        builder.append(colCount);
        builder.append(", columns=");
        builder.append(Arrays.toString(columns));
        builder.append("}");
        return builder.toString();
    }

    public static class PGColumn {
        /**
         * 字段名字
         */
        private String name = "";
        /**
         * 如果字段可以标识为一个特定表的字段，那么就是表的对象 ID ；否则就是零
         */
        private int tableOid = 0;
        /**
         * 如果该字段可以标识为一个特定表的字段，那么就是该表字段的属性号；否则就是零
         * 实际是列在表中的序号，从1开始
         */
        private int colNo = 0;
        /**
         * 字段数据类型的对象 ID
         */
        private int typeOid = 0;
        /**
         * 数据类型尺寸，负数表示变宽类型
         */
        private int typeLen = 0;
        /**
         * 类型修饰词
         */
        private int typeMod = 0;
        /**
         * 用于该字段的格式码。目前会是0(文本)或者1(二进制)
         */
        private int format = 0;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getTableOid() {
            return tableOid;
        }

        public void setTableOid(int tableOid) {
            this.tableOid = tableOid;
        }

        public int getColNo() {
            return colNo;
        }

        public void setColNo(int colNo) {
            this.colNo = colNo;
        }

        public int getTypeOid() {
            return typeOid;
        }

        public void setTypeOid(int typeOid) {
            this.typeOid = typeOid;
        }

        public int getTypeLen() {
            return typeLen;
        }

        public void setTypeLen(int typeLen) {
            this.typeLen = typeLen;
        }

        public int getTypeMod() {
            return typeMod;
        }

        public void setTypeMod(int typeMod) {
            this.typeMod = typeMod;
        }

        public int getFormat() {
            return format;
        }

        public void setFormat(int format) {
            this.format = format;
        }

        private List<byte[]> getPayload() {
            List<byte[]> bytes = new LinkedList<>();
            bytes.add(PGProto.buildNullStr(name));
            bytes.add(PGProto.buildInt32BE(tableOid));
            bytes.add(PGProto.buildInt16BE(colNo));
            bytes.add(PGProto.buildInt32BE(typeOid));
            bytes.add(PGProto.buildInt16BE(typeLen));
            bytes.add(PGProto.buildInt32BE(typeMod));
            bytes.add(PGProto.buildInt16BE(format));
            return bytes;
        }

        public byte[] toPacket() {
            List<byte[]> payload = this.getPayload();
            return PGMessage.toPacket(payload);
        }

        public static PGColumn loadFromPacket(byte[] packet) {
            PGProto proto = new PGProto(packet);
            PGColumn column = new PGColumn();
            column.setName(proto.readNullStr());
            column.setTableOid(proto.readInt32());
            column.setColNo(proto.readInt16());
            column.setTypeOid(proto.readInt32());
            column.setTypeLen(proto.readInt16());
            column.setTypeMod(proto.readInt32());
            column.setFormat(proto.readInt16());
            return column;
        }

        @Override public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("PGColumn {name=");
            builder.append(name);
            builder.append(", tableOid=");
            builder.append(tableOid);
            builder.append(", colNo=");
            builder.append(colNo);
            builder.append(", typeOid=");
            builder.append(typeOid);
            builder.append(", typeLen=");
            builder.append(typeLen);
            builder.append(", typeMod=");
            builder.append(typeMod);
            builder.append(", format=");
            builder.append(format);
            builder.append("}");
            return builder.toString();
        }

    }
}
