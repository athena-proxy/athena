package me.ele.jarch.athena;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Default implement of DecryptorSpi, just use base64 decrypt text.
 */
public class Base64Decryptor extends DecryptorSpi {
    @Override public void init() {
    }

    @Override public String decrypt(final String input) {
        byte[] src = input.getBytes(StandardCharsets.UTF_8);
        byte[] dst = Base64.getDecoder().decode(src);
        return new String(dst, StandardCharsets.UTF_8);
    }
}
