package me.ele.jarch.athena.pg.proto;

import java.util.Collections;
import java.util.List;

public class SSLRequestMessage extends PGMessage {
    public static final int SSL_REQUEST_NUMBER = 80877103;
    public static final int SSL_REQUEST_LENGTH = 8;
    // SSL请求包内部数据唯一
    public static final SSLRequestMessage SSL_REQUEST_MESSAGE = new SSLRequestMessage();

    public static final byte[] SSL_REQUEST_BUF = SSL_REQUEST_MESSAGE.toPacket();

    private SSLRequestMessage() {
    }

    @Override protected List<byte[]> getPayload() {
        return Collections.singletonList(PGProto.buildInt32BE(SSL_REQUEST_NUMBER));
    }

    @Override protected byte getTypeByte() {
        return PGFlags.C_SSL_REQUEST;
    }

    @Override public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("SSLRequestMessage {}");
        return builder.toString();
    }

}
