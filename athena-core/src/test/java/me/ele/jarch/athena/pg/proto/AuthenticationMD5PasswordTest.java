package me.ele.jarch.athena.pg.proto;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * Created by jinghao.wang on 16/11/23.
 */
public class AuthenticationMD5PasswordTest {

    @Test public void testToPacket() {
        AuthenticationMD5Password md5Password = new AuthenticationMD5Password();
        byte[] bytes = md5Password.toPacket();
        byte expectedType = PGFlags.AUTHENTICATION_MD5_PASSWORD;
        assertEquals(bytes[0], expectedType);
        int expectedTotalLength = 13;
        assertEquals(bytes.length, expectedTotalLength);
        PGProto proto = new PGProto(bytes, 1);
        int expectedPayloadLength = 12;
        assertEquals(proto.readInt32(), expectedPayloadLength);
        int expectedMd5Annotation = 5;
        assertEquals(proto.getInt32(5), expectedMd5Annotation);
    }

    @Test public void readPacket() {
        AuthenticationMD5Password md5Password = new AuthenticationMD5Password();
        byte[] bytes = md5Password.toPacket();
        AuthenticationMD5Password readMd5Password = AuthenticationMD5Password.loadFromPacket(bytes);
        assertEquals(md5Password.getSalt(), readMd5Password.getSalt());
    }

}
