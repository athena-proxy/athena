package com.github.mpjct.jmpjct.mysql.proto;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class Row extends Packet {
    public int type = Flags.ROW_TYPE_TEXT;
    public int colType = Flags.MYSQL_TYPE_VAR_STRING;
    public ArrayList<Object> data = new ArrayList<Object>();

    public Row() {
    }

    @Override public String toString() {
        StringBuilder sb = new StringBuilder();
        data.forEach(d -> {
            sb.append(d).append(",");
        });
        return sb.substring(0, sb.length() - 1);
    }

    public Row(String data1) {
        this.addData(data1);
    }

    public void addData(String data) {
        this.data.add(data);
    }

    public Row(String data1, Integer data2) {
        this.addData(data1);
        this.addData(data2);
    }

    public Row(String data1, long data2) {
        this.addData(data1);
        this.addData(data2);
    }

    public Row(String data1, float data2) {
        this.addData(data1);
        this.addData(data2);
    }

    public Row(String data1, boolean data2) {
        this.addData(data1);
        this.addData(data2);
    }

    public Row(String data1, String data2) {
        this.addData(data1);
        this.addData(data2);
    }

    public void addData(Integer data) {
        this.data.add(Integer.toString(data));
    }

    public void addData(long data) {
        this.data.add(String.valueOf(data));
    }

    public void addData(float data) {
        this.data.add(String.valueOf(data));
    }

    public void addData(boolean data) {
        this.data.add(String.valueOf(data));
    }

    // Add other addData for other types here

    public ArrayList<byte[]> getPayload() {
        ArrayList<byte[]> payload = new ArrayList<byte[]>();

        for (Object obj : this.data) {
            switch (this.type) {
                case Flags.ROW_TYPE_TEXT:
                    if (obj instanceof String)
                        payload.add(Proto.build_lenenc_str((String) obj));
                    else if (obj instanceof Integer)
                        payload.add(Proto.build_lenenc_int((Integer) obj));
                    else if (obj == null)
                        payload.add(Proto.build_lenenc_int(251));
                    else {
                        // trigger error
                    }
                    break;
                case Flags.ROW_TYPE_BINARY:
                    break;
                default:
                    break;
            }
        }

        return payload;
    }

    public static Row loadFromPacket(byte[] packet) {
        Row obj = new Row();
        Proto proto = new Proto(packet, 3);

        obj.sequenceId = proto.get_fixed_int(1);

        return obj;
    }

    public static Row loadFromPacket(byte[] packet, int colCount) {
        Row obj = new Row();
        Proto proto = new Proto(packet, 3);

        obj.sequenceId = proto.get_fixed_int(1);
        for (int i = 0; i < colCount; i++) {
            obj.addData(proto.get_lenenc_str());//may be null for sum
        }

        return obj;
    }

    public static List<String> loadColumnsFromPacket(final byte[] packet) {
        List<String> columns = new LinkedList<>();
        Proto proto = new Proto(packet);
        long packetLen = proto.get_fixed_int(3);
        proto.get_fixed_int(1);

        while (proto.offset <= packetLen) {
            columns.add(proto.get_lenenc_str());
        }

        return columns;

    }
}
