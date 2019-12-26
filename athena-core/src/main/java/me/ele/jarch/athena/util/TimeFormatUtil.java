package me.ele.jarch.athena.util;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class TimeFormatUtil {
    public static final ThreadLocal<SimpleDateFormat> sdfs =
        ThreadLocal.withInitial(() -> new SimpleDateFormat("HH:mm:ss.SSS"));

    private static final DateTimeFormatter dateTimeFormatter =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS").withZone(ZoneId.systemDefault());

    public static void appendTime(String name, StringBuilder sb, long t) {
        SimpleDateFormat sdf = sdfs.get();
        appendTime(name, sb, sdf, t);
    }

    public static void appendTime(String name, StringBuilder sb, SimpleDateFormat sdf, long t) {
        if (t == 0) {
            sb.append(name).append("0").append(",");
        } else {
            sb.append(name).append(sdf.format(new Date(t))).append(",");
        }
    }

    public static String formatTimeStamp(long t) {
        SimpleDateFormat sdf = sdfs.get();
        return sdf.format(new Date(t));
    }

    public static String convert2Datetime(long timestamp) {
        Instant instant = Instant.ofEpochMilli(timestamp);
        return dateTimeFormatter.format(instant);
    }
}
