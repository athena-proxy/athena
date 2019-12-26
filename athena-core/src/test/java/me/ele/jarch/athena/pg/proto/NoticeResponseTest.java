package me.ele.jarch.athena.pg.proto;

import me.ele.jarch.athena.pg.proto.NoticeResponse.Notice;
import me.ele.jarch.athena.pg.util.Severity;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author shaoyang.qi
 */
public class NoticeResponseTest {
    @Test public void testNoticeToPacket() {
        byte code = PGFlags.SEVERITY;
        String msg = Severity.WARNING.name();
        Notice notice = new Notice(code, msg);
        byte[] bytes = notice.toPacket();

        int expectedDataLen = 1 + msg.length() + 1;
        assertEquals(bytes.length, expectedDataLen);
        PGProto proto = new PGProto(bytes);
        assertEquals(proto.readByte(), code);
        assertEquals(proto.readNullStr(), msg);
    }

    @Test public void testToPacket() {
        byte code = PGFlags.SEVERITY;
        String msg = Severity.WARNING.name();
        NoticeResponse noticeResp = new NoticeResponse();
        noticeResp.addNotice(code, msg);
        byte[] bytes = noticeResp.toPacket();

        int expectedDataTotalLen = 1 + 4 + 1 + msg.length() + 1 + 1;
        assertEquals(bytes.length, expectedDataTotalLen);

        PGProto proto = new PGProto(bytes);

        byte expectedDataType = PGFlags.NOTICE_RESPONSE;
        assertEquals(proto.readByte(), expectedDataType);

        int expectedDataPayloadLen = expectedDataTotalLen - 1;
        assertEquals(proto.readInt32(), expectedDataPayloadLen);

        assertEquals(proto.readByte(), code);
        assertEquals(proto.readNullStr(), msg);
    }

    @Test public void testLoadFromPacket() {
        byte code = PGFlags.SEVERITY;
        String msg = Severity.WARNING.name();
        NoticeResponse noticeResp = new NoticeResponse();
        noticeResp.addNotice(code, msg);
        byte[] bytes = noticeResp.toPacket();

        NoticeResponse newNotice = NoticeResponse.loadFromPacket(bytes);
        assertEquals(newNotice.toPacket(), bytes);
        assertEquals(newNotice.getNotices().size(), 1);
        Notice notice = newNotice.getNotices().get(0);
        assertEquals(notice.code, code);
        assertEquals(notice.getNotice(), msg);
    }
}
