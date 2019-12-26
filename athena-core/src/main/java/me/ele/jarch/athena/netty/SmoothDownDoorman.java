package me.ele.jarch.athena.netty;

import me.ele.jarch.athena.util.AthenaConfig;
import org.apache.commons.codec.digest.DigestUtils;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Base64;

/**
 * tokenBytes = new String(smoothDownPassword + date(yyyyMMdd)).getBytes(StandardCharsets.UTF_8)
 * token = new String(Base64.getEncoder.encode(tokenBytes), StandardCharsets.UTF_8)
 * <p>
 * Created by jinghao.wang on 16/9/18.
 */
public class SmoothDownDoorman {

    private final String secretKey;

    private SmoothDownDoorman(String secretKey) {
        this.secretKey = secretKey;
    }

    private static class LazyHolder {
        private static final SmoothDownDoorman INSTANCE =
            new SmoothDownDoorman(AthenaConfig.getInstance().getSmoothDownPwd());
    }

    public static SmoothDownDoorman getInstance() {
        return LazyHolder.INSTANCE;
    }

    public boolean allowSmoothDown(String token) {
        byte[] decodedBytes = Base64.getDecoder().decode(token);
        String decodedString = new String(decodedBytes, StandardCharsets.UTF_8);
        if (decodedString.length() <= 8) {
            return false;
        }
        String dateText =
            decodedString.substring(decodedString.length() - 8, decodedString.length());
        String password = decodedString.substring(0, decodedString.length() - 8);
        return checkDate(dateText) && checkPassword(password);
    }

    private boolean checkDate(String dataText) {
        LocalDate expectDate = LocalDate.now();
        String expectDateText = expectDate.format(DateTimeFormatter.BASIC_ISO_DATE);
        return expectDateText.equals(dataText);
    }

    private boolean checkPassword(String password) {
        String secret = DigestUtils.md5Hex(password);
        return secretKey.equalsIgnoreCase(secret);
    }
}
