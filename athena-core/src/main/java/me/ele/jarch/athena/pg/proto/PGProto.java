package me.ele.jarch.athena.pg.proto;

import java.nio.charset.StandardCharsets;

/**
 * Created by jinghao.wang on 16/11/23.
 */
public class PGProto {
    private final byte[] packet;
    private int offset;

    public PGProto(byte[] packet, int offset) {
        this.packet = packet;
        this.offset = offset;
    }

    public PGProto(byte[] packet) {
        this(packet, 0);
    }

    public boolean hasRemaining() {
        return offset < packet.length;
    }

    public byte[] readBytes(int size) {
        if (size < 0) {
            return null;
        }
        size = offset + size > packet.length ? (packet.length - offset) : size;
        byte[] bytes = new byte[size];
        System.arraycopy(packet, offset, bytes, 0, size);
        offset += size;
        return bytes;
    }

    public byte readByte() {
        byte result = getByte(offset);
        offset += 1;
        return result;
    }

    public byte getByte(int offset) {
        return packet[offset];
    }

    public int readInt8() {
        int value = (int) getByte(offset);
        offset += 1;
        return value;
    }

    public int readInt16() {
        int value = getInt16(offset);
        offset += 2;
        return value;
    }

    public int getInt16(int offset) {
        return (packet[offset] << 8 | packet[offset + 1] & 0xff);
    }

    public int readInt32() {
        int value = getInt32(offset);
        offset += 4;
        return value;
    }

    public int getInt32(int offset) {
        return (packet[offset] & 0xff) << 24 | (packet[offset + 1] & 0xff) << 16
            | (packet[offset + 2] & 0xff) << 8 | packet[offset + 3] & 0xff;
    }

    public String readNullStr() {
        int endIndex = offset;
        for (int i = offset; i < packet.length; i++) {
            if (packet[i] == 0x00) {
                endIndex = i;
                break;
            }
        }
        int length = endIndex - offset + 1;
        String value = readFixedStr(offset, length - 1);
        offset += length;
        return value;
    }

    public String readFixedStr(int length) {
        String value = readFixedStr(offset, length);
        offset += length;
        return value;
    }

    public String readFixedStr(int offset, int length) {
        return new String(packet, offset, length, StandardCharsets.UTF_8);
    }

    public static byte[] buildInt16BE(final int value) {
        byte[] bytes = new byte[2];
        bytes[0] = (byte) (value >>> 8);
        bytes[1] = (byte) value;
        return bytes;
    }

    public static byte[] buildInt32BE(final int value) {
        byte[] bytes = new byte[4];
        bytes[0] = (byte) (value >>> 24);
        bytes[1] = (byte) (value >>> 16);
        bytes[2] = (byte) (value >>> 8);
        bytes[3] = (byte) value;
        return bytes;
    }

    public static byte[] buildNullStr(String str) {
        byte[] strBytes = str.getBytes(StandardCharsets.UTF_8);
        byte[] bytes = new byte[strBytes.length + 1];
        System.arraycopy(strBytes, 0, bytes, 0, strBytes.length);
        return bytes;
    }
}
