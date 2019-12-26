package me.ele.jarch.athena.pg.proto;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by jinghao.wang on 16/11/26.
 */
public class NotificationResponse extends PGMessage {
    private int pid;
    private String channel;
    private String content;

    @Override protected List<byte[]> getPayload() {
        List<byte[]> payload = new LinkedList<>();
        payload.add(PGProto.buildInt32BE(pid));
        payload.add(PGProto.buildNullStr(channel));
        payload.add(PGProto.buildNullStr(content));
        return payload;
    }

    @Override protected byte getTypeByte() {
        return PGFlags.NOTIFICATION_RESPONSE;
    }

    public NotificationResponse(int pid, String channel, String content) {
        this.channel = channel;
        this.content = content;
        this.pid = pid;
    }

    public NotificationResponse() {
    }

    public int getPid() {
        return pid;
    }

    public void setPid(int pid) {
        this.pid = pid;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public static NotificationResponse loadFromPacket(byte[] packet) {
        PGProto proto = new PGProto(packet, 5);
        int pid = proto.readInt32();
        String channel = proto.readNullStr();
        String content = proto.readNullStr();
        return new NotificationResponse(pid, channel, content);
    }

    @Override public String toString() {
        final StringBuilder sb = new StringBuilder("NotificationResponse{");
        sb.append("pid=").append(pid);
        sb.append(", channel='").append(channel).append('\'');
        sb.append(", content='").append(content).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
