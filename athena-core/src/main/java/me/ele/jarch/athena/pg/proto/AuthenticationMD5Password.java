package me.ele.jarch.athena.pg.proto;

import com.github.mpjct.jmpjct.util.RandomUtil;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by jinghao.wang on 16/11/23.
 */
public class AuthenticationMD5Password extends Authentication {
    private byte[] salt;

    @Override protected List<byte[]> getPayload() {
        List<byte[]> bytes = new LinkedList<>();
        bytes.add(PGProto.buildInt32BE(authType.getAuthTypeNum()));
        bytes.add(salt);
        return bytes;
    }

    @Override protected byte getTypeByte() {
        return PGFlags.AUTHENTICATION_MD5_PASSWORD;
    }

    public AuthenticationMD5Password(byte[] salt) {
        super(AuthType.AuthenticationMD5Password);
        this.salt = salt;
    }

    public AuthenticationMD5Password() {
        this(RandomUtil.randomBytes(4));
    }

    public byte[] getSalt() {
        return salt;
    }

    public void setSalt(byte[] salt) {
        this.salt = salt;
    }

    public static AuthenticationMD5Password loadFromPacket(byte[] packet) {
        byte[] salt = new byte[4];
        System.arraycopy(packet, 9, salt, 0, 4);
        return new AuthenticationMD5Password(salt);
    }

    @Override public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("AuthenticationMD5Password {salt=");
        builder.append(Arrays.toString(salt));
        builder.append("}");
        return builder.toString();
    }

}
