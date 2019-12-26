package me.ele.jarch.athena;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * SQLLogFilter.error(msg) will not upload into kafka
 * <p>
 * Useful to log SQL and filter the sensitive information
 */
public class SQLLogFilter {
    private static final Logger logger = LoggerFactory.getLogger(SQLLogFilter.class);

    public static void error(String msg) {
        logger.error(msg);
    }

    public static void error(String msg, Map<String, Object> extension) {
        logger.error(msg, extension);
    }
}
