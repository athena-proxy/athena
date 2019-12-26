package me.ele.jarch.athena.pg.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by jinghao.wang on 16/11/24.
 */
public class MD5Digest {
    private MD5Digest() {
    }

    // 用于生成dal-credentials.cfg里加密后的密码
    public static String encodeForDalCredentialsCfg(String user, String password) {
        MessageDigest md;
        byte[] pass_digest;
        byte[] hex_digest = new byte[32];

        try {
            md = MessageDigest.getInstance("MD5");

            md.update(password.getBytes(StandardCharsets.UTF_8));
            md.update(user.getBytes(StandardCharsets.UTF_8));
            pass_digest = md.digest();

            bytesToHex(pass_digest, hex_digest, 0);
            return new String(hex_digest, StandardCharsets.UTF_8);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Unable to encode password with MD5", e);
        }
    }

    /**
     * Encodes user/password/salt information in the following way: MD5(MD5(password + user) + salt)
     *
     * @param user     The connecting user.
     * @param password The connecting user's password.
     * @param salt     A four-salt sent by the server.
     * @return A 35-byte array, comprising the string "md5" and an MD5 digest.
     */
    public static byte[] encode(byte user[], byte password[], byte salt[]) {
        MessageDigest md;
        byte[] temp_digest;
        byte[] pass_digest;
        byte[] hex_digest = new byte[35];

        try {
            md = MessageDigest.getInstance("MD5");

            md.update(password);
            md.update(user);
            temp_digest = md.digest();

            bytesToHex(temp_digest, hex_digest, 0);
            md.update(hex_digest, 0, 32);
            md.update(salt);
            pass_digest = md.digest();

            bytesToHex(pass_digest, hex_digest, 3);
            hex_digest[0] = (byte) 'm';
            hex_digest[1] = (byte) 'd';
            hex_digest[2] = (byte) '5';
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Unable to encode password with MD5", e);
        }

        return hex_digest;
    }

    /**
     * Encodes user/password/salt information in the following way: MD5(MD5(password + user) + salt)
     *
     * @param pwdUserMD5 MD5(password + user)
     * @param salt       A four-salt sent by the server.
     * @return A 35-byte array, comprising the string "md5" and an MD5 digest.
     */
    public static byte[] encode(byte[] pwdUserMD5, byte[] salt) {
        MessageDigest md;
        byte[] pass_digest;
        byte[] hex_digest = new byte[35];

        try {
            md = MessageDigest.getInstance("MD5");

            md.update(pwdUserMD5, 0, 32);
            md.update(salt);
            pass_digest = md.digest();

            bytesToHex(pass_digest, hex_digest, 3);
            hex_digest[0] = (byte) 'm';
            hex_digest[1] = (byte) 'd';
            hex_digest[2] = (byte) '5';
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Unable to encode password with MD5", e);
        }

        return hex_digest;
    }

    /*
     * Turn 16-byte stream into a human-readable 32-byte hex string
     */
    private static void bytesToHex(byte[] bytes, byte[] hex, int offset) {
        final char lookup[] =
            {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

        int i;
        int c;
        int j;
        int pos = offset;

        for (i = 0; i < 16; i++) {
            c = bytes[i] & 0xFF;
            j = c >> 4;
            hex[pos++] = (byte) lookup[j];
            j = (c & 0xF);
            hex[pos++] = (byte) lookup[j];
        }
    }
}
