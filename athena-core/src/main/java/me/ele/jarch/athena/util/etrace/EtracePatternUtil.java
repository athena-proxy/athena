package me.ele.jarch.athena.util.etrace;

import me.ele.jarch.athena.constant.Metrics;
import me.ele.jarch.athena.netty.AthenaServer;
import me.ele.jarch.athena.util.AthenaConfig;
import me.ele.jarch.athena.util.GreySwitch;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Created by jinghao.wang on 16/6/27.
 */
public class EtracePatternUtil {
    private static final Logger logger = LoggerFactory.getLogger(EtracePatternUtil.class);

    private static final String PATTERN_MAP_TEMPLATE = "%s#%s";

    /* 控制发送SQL Pattern 到Etrace的比率, 范围(0~1000), 由FakeSql 修改 */
    private static volatile int sqlPatternThreshold = 1;
    /* 平均SQL pattern按照最大4k字节估算, 30720条pattern大约使用120M的内存空间*/
    public static final int MAX_CACHED_PATTERNS = 30270;
    private static final Map<String, SQLRecord> sqlPatternRecords = new ConcurrentSkipListMap<>();
    private static final AtomicInteger count = new AtomicInteger(0);
    public static final AtomicBoolean flushed2disk = new AtomicBoolean(false);

    private EtracePatternUtil() {
        //NOOP
    }

    public static class SQLRecord {
        public final String hash;
        public final String sqlPattern;
        private volatile long count = 0;
        private volatile long sumResponseTimeInMill = 0;
        private volatile boolean sent2Trace = false;
        private volatile long lastCheckedInMill = System.currentTimeMillis();

        public boolean isSent2Trace() {
            long currentTime = System.currentTimeMillis();
            // refresh after one day, despite it sent or not.
            if (lastCheckedInMill < currentTime - 24 * 3600 * 1000) {
                sent2Trace = false;
                lastCheckedInMill = currentTime;
            }

            return sent2Trace;
        }

        public void markAsSent2Trace() {
            this.sent2Trace = true;
        }

        public SQLRecord(String hash, String sqlPattern) {
            this.hash = hash;
            this.sqlPattern = sqlPattern;
        }

        public void addResponseTime(long responseTimeInMill) {
            ++count;
            this.sumResponseTimeInMill += responseTimeInMill;
        }

        public long getCount() {
            return count;
        }

        public long getSumResponseTimeInMill() {
            return sumResponseTimeInMill;
        }
    }


    static {
        AthenaServer.commonJobScheduler.addJob("flush.sqlpattern.with.hash", 1 * 60 * 1000, () -> {
            if (!flushed2disk.compareAndSet(false, true)) {
                // no new sqlPattern since last flush2disk, no needs to write to disk again
                return;
            }

            String globalLogPath = AthenaConfig.getInstance().getGlobalLogPath();
            Path patternMapFilePath = Paths.get(globalLogPath, "athena_pattern_map.log");
            try {
                if (!Files.exists(patternMapFilePath)) {
                    Files.createFile(patternMapFilePath);
                }
                List<String> patternMaps = getPatternMaps();
                // the Files will add StandardOpenOption.WRITE option default.
                Files.write(patternMapFilePath, patternMaps, StandardOpenOption.TRUNCATE_EXISTING);
            } catch (Exception e) {
                flushed2disk.set(false);
                logger.error("Failed to write to " + patternMapFilePath, e);
            }
        });
    }

    public static SQLRecord addAndGet(String sqlPattern) {
        return addAndGet(sqlPattern, () -> DigestUtils.md5Hex(sqlPattern));
    }

    public static SQLRecord addAndGet(String sqlPattern, Supplier<String> sqlHashSupplier) {
        Objects.requireNonNull(sqlPattern, "sql pattern must not be  null");
        SQLRecord sqlRecord = sqlPatternRecords.get(sqlPattern);
        if (sqlRecord != null) {
            return sqlRecord;
        }
        assertMaxCachedPatterns();
        sqlRecord = new SQLRecord(sqlHashSupplier.get(), sqlPattern);
        // 如果sqlPattern太长,则不存储pattern到内存中,直接返回临时生成的sqlRecord
        if (isSqlPatternTooLong(sqlPattern)) {
            return sqlRecord;
        }
        sqlPatternRecords.put(sqlPattern, sqlRecord);
        count.incrementAndGet();
        flushed2disk.set(false);
        return sqlRecord;
    }

    public static boolean isSqlPatternTooLong(final String sqlPattern) {
        return sqlPattern.length() > GreySwitch.getInstance().getMaxSqlPatternLength();
    }

    public static String sqlPatternInfo() {

        StringBuilder builder = new StringBuilder();
        builder.ensureCapacity(1024 * 1024);

        // hash averageTime count sumTime --- sqlPattern
        sqlPatternRecords.entrySet().stream().sorted((a, b) -> {
            double average_a = a.getValue().sumResponseTimeInMill / (a.getValue().count + 0.001d);
            double average_b = b.getValue().sumResponseTimeInMill / (b.getValue().count + 0.001d);
            return Double.compare(average_a, average_b);
        }).forEach(i -> {
            SQLRecord record = i.getValue();
            builder.append(String
                .format("%8dms ", (long) (record.sumResponseTimeInMill / (record.count + 0.001d))));
            builder.append(String.format("%16d ", record.count));
            builder.append(String.format("%16d ", record.sumResponseTimeInMill));
            builder.append(String.format(" --- %40s", i.getKey()));
            builder.append(String.format(" %s", record.sqlPattern));
            builder.append("\n");
        });

        return builder.toString();
    }

    private static void assertMaxCachedPatterns() {
        if (count.get() >= MAX_CACHED_PATTERNS) {
            MetricFactory.newCounter(Metrics.SQLPATTERN_TOO_MUCH).once();
            logger.error("dangerous! cached sql PATTERN_HASH_CACHE exceed max count: {}",
                MAX_CACHED_PATTERNS);
            sqlPatternRecords.clear();
            count.set(0);
        }
    }

    static List<String> getPatternMaps() {
        return sqlPatternRecords.entrySet().stream()
            .sorted((entry1, entry2) -> entry1.getKey().compareTo(entry2.getKey())).map(
                entry -> String.format(PATTERN_MAP_TEMPLATE, entry.getValue().hash, entry.getKey()))
            .collect(Collectors.toList());
    }

    public static int getSqlPatternThreshold() {
        return sqlPatternThreshold;
    }

    public static void setSqlPatternThreshold(int sqlPatternThreshold) {
        EtracePatternUtil.sqlPatternThreshold = sqlPatternThreshold;
    }
}
