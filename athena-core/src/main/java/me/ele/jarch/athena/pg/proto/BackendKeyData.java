package me.ele.jarch.athena.pg.proto;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by jinghao.wang on 16/11/24.
 */
public class BackendKeyData extends PGMessage {
    private int pid;
    private int key;

    @Override protected List<byte[]> getPayload() {
        List<byte[]> payload = new LinkedList<>();
        payload.add(PGProto.buildInt32BE(pid));
        payload.add(PGProto.buildInt32BE(key));
        return payload;
    }

    @Override protected byte getTypeByte() {
        return PGFlags.BACKEND_KEY_DATA;
    }

    @SuppressWarnings("unused") private BackendKeyData() {
        //disable default constructor
    }

    public BackendKeyData(int pid, int key) {
        this.pid = pid;
        this.key = key;
    }

    public int getKey() {
        return key;
    }

    public void setKey(int key) {
        this.key = key;
    }

    public int getPid() {
        return pid;
    }

    public void setPid(int pid) {
        this.pid = pid;
    }

    public static int genKey() {
        return ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE);
    }

    public static BackendKeyData loadFromPacket(byte[] packet) {
        PGProto proto = new PGProto(packet, 5);
        return new BackendKeyData(proto.readInt32(), proto.readInt32());
    }

    @Override public String toString() {
        final StringBuilder sb = new StringBuilder("BackendKeyData{");
        sb.append("pid=").append(pid);
        sb.append(", key=").append(key);
        sb.append('}');
        return sb.toString();
    }
}
