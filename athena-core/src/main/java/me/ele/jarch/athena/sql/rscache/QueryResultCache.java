package me.ele.jarch.athena.sql.rscache;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Created by shaoyang.qi on 2017/9/12.
 * 此类用于存储查询语句的结果，提高查询效率
 * 目前支持缓存show collation,show variables,select @@XXX这种结果极少变化的sql
 */
public class QueryResultCache {
    private static final int MAX_CACHE_SIZE = 100;
    private static final long EXPIRED_TIME_IN_MILLIS = TimeUnit.SECONDS.toMillis(30);
    //<sql,resultSets>
    private final Map<String, CacheRecord> variableResults = new ConcurrentHashMap<>();
    private int count = 0;

    public Optional<List<byte[]>> get(String sql) {
        CacheRecord record = variableResults.getOrDefault(sql, CacheRecord.EMPTY);
        if (record == CacheRecord.EMPTY || record.isExpired(EXPIRED_TIME_IN_MILLIS)) {
            return Optional.empty();
        }
        return Optional.of(record.packets);
    }

    public synchronized void put(String sql, List<byte[]> packets) {
        if (count >= MAX_CACHE_SIZE) {
            variableResults.clear();
            count = 0;
        }
        CacheRecord old = variableResults.put(sql, new CacheRecord(packets));
        if (Objects.isNull(old)) {
            count++;
        }
    }

}
