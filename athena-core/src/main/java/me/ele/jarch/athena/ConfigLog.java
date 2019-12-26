package me.ele.jarch.athena;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by dan.he02 on 2016/12/14.
 * record log when
 * 1. load config file
 * 2. config change
 * 3. config error
 * keep original log level
 */
public class ConfigLog {
    // true: log to config.log,and log to athena.log
    // false: log to athena.log as usual
    public static final AtomicBoolean configLogOn = new AtomicBoolean(true);
    private static Logger logger = LoggerFactory.getLogger(ConfigLog.class);

    public static void error(String msg) {
        if (configLogOn.get()) {
            logger.error(msg);
        }
    }

    public static void info(String msg) {
        if (configLogOn.get()) {
            logger.info(msg);
        }
    }

    public static void warn(String msg) {
        if (configLogOn.get()) {
            logger.warn(msg);
        }
    }

    public static void error(String msg, Throwable t) {
        if (configLogOn.get()) {
            logger.error(msg, t);
        }
    }

    public static void info(String msg, Throwable t) {
        if (configLogOn.get()) {
            logger.info(msg, t);
        }
    }

    public static void warn(String msg, Throwable t) {
        if (configLogOn.get()) {
            logger.warn(msg, t);
        }
    }
}
