package me.ele.jarch.athena.pg.proto;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * See <a href="https://www.postgresql.org/docs/9.6/static/protocol-message-formats.html">Postgresql Message Formats</a>
 * Created by jinghao.wang on 16/11/23.
 */
public abstract class PGMessage {
    private static final Logger LOGGER = LoggerFactory.getLogger(PGMessage.class);

    protected abstract byte getTypeByte();

    protected abstract List<byte[]> getPayload();

    public byte[] toPacket() {
        List<byte[]> payload = this.getPayload();
        byte typeByte = getTypeByte();

        int size = 4;
        for (byte[] field : payload)
            size += field.length;

        byte[] packet = typeByte < 0 ? new byte[size] : new byte[size + 1];

        int offset = 0;
        if (typeByte < 0) {
            System.arraycopy(PGProto.buildInt32BE(size), 0, packet, offset, 4);
            offset += 4;
        } else {
            packet[0] = typeByte;
            offset += 1;
            System.arraycopy(PGProto.buildInt32BE(size), 0, packet, offset, 4);
            offset += 4;
        }

        for (byte[] field : payload) {
            System.arraycopy(field, 0, packet, offset, field.length);
            offset += field.length;
        }

        return packet;
    }

    public static byte[] toPacket(List<byte[]> payload) {
        if (payload == null) {
            return null;
        }
        int size = 0;
        for (byte[] field : payload) {
            size += field.length;
        }
        byte[] packet = new byte[size];
        int offset = 0;
        for (byte[] field : payload) {
            System.arraycopy(field, 0, packet, offset, field.length);
            offset += field.length;
        }
        return packet;

    }

    public static byte getType(byte[] packet) {
        return packet[0];
    }

    public static String getClientPacketTypeName(byte type) {
        String rt = PG_CLIENT_COM_MAP.get(type);
        if (rt == null) {
            return "Out of Bounds type: " + type;
        }
        return rt;
    }

    private static final Map<Byte, String> PG_CLIENT_COM_MAP = new HashMap<>();

    static {
        for (Field f : PGFlags.class.getDeclaredFields()) {
            if (f.getName().startsWith("C_") || f.getName().startsWith("B_")) {
                try {
                    PG_CLIENT_COM_MAP.put(f.getByte(null), f.getName());
                } catch (Exception e) {
                    LOGGER.error("", e);
                }
            }
        }
    }
}
