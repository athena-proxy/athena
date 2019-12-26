package me.ele.jarch.athena.pg.proto;

import java.util.LinkedList;
import java.util.List;

/**
 * @author shaoyang.qi
 */
public class NoticeResponse extends PGMessage {
    private List<Notice> notices = new LinkedList<>();

    @Override protected byte getTypeByte() {
        return PGFlags.NOTICE_RESPONSE;
    }

    @Override protected List<byte[]> getPayload() {
        List<byte[]> bytes = new LinkedList<>();
        for (Notice notice : notices) {
            bytes.add(notice.toPacket());
        }
        bytes.add(new byte[] {0x00});
        return bytes;
    }

    public NoticeResponse() {
    }

    public NoticeResponse(List<Notice> notices) {
        setNotices(notices);
    }

    public List<Notice> getNotices() {
        return notices;
    }

    public void setNotices(List<Notice> notices) {
        this.notices = notices;
    }

    public boolean addNotice(byte code, String noticeMsg) {
        if (notices == null) {
            return false;
        }
        notices.add(new Notice(code, noticeMsg));
        return true;
    }

    public static NoticeResponse loadFromPacket(byte[] packet) {
        PGProto proto = new PGProto(packet, 5);
        List<Notice> notices = new LinkedList<>();
        while (true) {
            byte code = proto.readByte();
            if (code == 0x0) {
                break;
            }
            String noticeStr = proto.readNullStr();
            notices.add(new Notice(code, noticeStr));
        }
        return new NoticeResponse(notices);
    }

    @Override public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("NoticeResponse {notices=");
        builder.append(notices);
        builder.append("}");
        return builder.toString();
    }

    public static class Notice {
        public final byte code;
        private String notice = "";

        public Notice(byte code) {
            this.code = code;
        }

        public Notice(byte code, String notice) {
            this.code = code;
            setNotice(notice);
        }

        public String getNotice() {
            return notice;
        }

        public void setNotice(String notice) {
            this.notice = notice;
        }

        private List<byte[]> getPayload() {
            List<byte[]> bytes = new LinkedList<>();
            bytes.add(new byte[] {code});
            if (code == 0x0) {
                return bytes;
            }
            bytes.add(PGProto.buildNullStr(notice));
            return bytes;
        }

        public byte[] toPacket() {
            List<byte[]> payload = this.getPayload();
            return PGMessage.toPacket(payload);
        }

        @Override public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("Notice {code=");
            builder.append(code);
            builder.append(", notice=");
            builder.append(notice);
            builder.append("}");
            return builder.toString();
        }

    }
}
