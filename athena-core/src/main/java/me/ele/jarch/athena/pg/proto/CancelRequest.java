package me.ele.jarch.athena.pg.proto;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by jinghao.wang on 16/11/26.
 */
public class CancelRequest extends PGMessage {
    public static final int CANCEL_REQUEST_CODE = 80877102;
    public static final int CANCEL_REQUEST_LENGTH = 16;
    private int pid;
    private int key;

    @Override protected byte getTypeByte() {
        return PGFlags.C_CANCEL_REQUEST;
    }

    @Override protected List<byte[]> getPayload() {
        List<byte[]> payload = new LinkedList<>();
        payload.add(PGProto.buildInt32BE(CANCEL_REQUEST_CODE));
        payload.add(PGProto.buildInt32BE(pid));
        payload.add(PGProto.buildInt32BE(key));
        return payload;
    }

    public CancelRequest(int pid, int key) {
        this.pid = pid;
        this.key = key;
    }

    public int getPid() {
        return pid;
    }

    public void setPid(int pid) {
        this.pid = pid;
    }

    public int getKey() {
        return key;
    }

    public void setKey(int key) {
        this.key = key;
    }

    public static CancelRequest loadFromPacket(byte[] packet) {
        return _loadFromPacket(packet, false);
    }

    public static CancelRequest loadFromVirtualPacket(byte[] packet) {
        return _loadFromPacket(packet, true);
    }

    private static CancelRequest _loadFromPacket(byte[] packet, boolean isVirtual) {
        PGProto proto = new PGProto(packet, isVirtual ? 9 : 8);
        int pid = proto.readInt32();
        int key = proto.readInt32();
        return new CancelRequest(pid, key);
    }

    @Override public String toString() {
        final StringBuilder sb = new StringBuilder("CancelRequest{");
        sb.append("pid=").append(pid);
        sb.append(", key=").append(key);
        sb.append('}');
        return sb.toString();
    }
}
