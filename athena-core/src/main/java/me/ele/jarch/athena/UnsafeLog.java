package me.ele.jarch.athena;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class UnsafeLog {
    // true: log to unsafe.log,not log to athena.log
    // false: log to athena.log as usual
    public static final AtomicBoolean unsafeLogOn = new AtomicBoolean(false);
    private static Logger logger = LoggerFactory.getLogger(UnsafeLog.class);

    public static void error(String msg) {
        logger.error(msg);
    }
}
