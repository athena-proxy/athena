package me.ele.jarch.athena.sharding.sql;

import me.ele.jarch.athena.util.etrace.EtracePatternUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;

public class SQLPatternAnalyzer {
    private static final Logger logger = LoggerFactory.getLogger(SQLPatternAnalyzer.class);
    private static final int MAX_LENGTH = 1024 * 256;

    private final LinkedBlockingQueue<SQLPatternDurationPair> patternBuffer =
        new LinkedBlockingQueue<>(MAX_LENGTH);


    private static class SQLPatternDurationPair {
        private String pattern;
        private long duration;

        public SQLPatternDurationPair(String pattern, long duration) {
            this.pattern = pattern;
            this.duration = duration;
        }
    }


    private static final SQLPatternAnalyzer inst = new SQLPatternAnalyzer();

    public static SQLPatternAnalyzer getInst() {
        return inst;
    }

    private SQLPatternAnalyzer() {
        new Thread(() -> {
            while (true) {
                try {
                    SQLPatternDurationPair sqlDurPair = patternBuffer.take();
                    consumeSQLPattern(sqlDurPair);
                } catch (InterruptedException t) {
                    logger.error("Exception caught when background thread working!", t);
                }
            }
        }, "SQLPatternAnalyzer-thread").start();
    }

    public void produceSQLPattern(String sql, long duration) {
        if (Objects.isNull(sql) || "".equals(sql) || duration < 0) {
            logger.error(String.format("sql=%s,duration=%d", sql, duration));
            return;
        }
        if (!patternBuffer.offer(new SQLPatternDurationPair(sql, duration))) {
            logger.error("pattern buffer is full");
        }
    }

    private void consumeSQLPattern(SQLPatternDurationPair sqlDurPair) {
        EtracePatternUtil.SQLRecord sqlRecord = EtracePatternUtil.addAndGet(sqlDurPair.pattern);
        sqlRecord.addResponseTime(sqlDurPair.duration);
    }
}
