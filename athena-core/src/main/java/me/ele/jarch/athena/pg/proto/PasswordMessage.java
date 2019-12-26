package me.ele.jarch.athena.pg.proto;

import me.ele.jarch.athena.pg.util.MD5Digest;

import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by jinghao.wang on 16/11/23.
 */
public class PasswordMessage extends PGMessage {
    private final String password;

    @Override protected List<byte[]> getPayload() {
        List<byte[]> payload = new LinkedList<>();
        payload.add(password.getBytes(StandardCharsets.UTF_8));
        payload.add(new byte[] {0x00});
        return payload;
    }

    @Override protected byte getTypeByte() {
        return PGFlags.C_PASSWORD_MESSAGE;
    }

    public PasswordMessage(byte[] password) {
        this(new String(password, StandardCharsets.UTF_8));
    }

    public PasswordMessage(String password) {
        this.password = password;
    }

    public PasswordMessage(String user, String password, byte[] salt) {
        this(user.getBytes(StandardCharsets.UTF_8), password.getBytes(StandardCharsets.UTF_8),
            salt);
    }

    public PasswordMessage(byte[] user, byte[] password, byte[] salt) {
        this(MD5Digest.encode(user, password, salt));
    }

    public String getPassword() {
        return password;
    }

    public static PasswordMessage loadFromPacket(byte[] packet) {
        PGProto proto = new PGProto(packet, 5);
        String password = proto.readNullStr();
        return new PasswordMessage(password);
    }

    @Override public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("PasswordMessage {password=");
        builder.append(password);
        builder.append("}");
        return builder.toString();
    }

}
