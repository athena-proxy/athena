package me.ele.jarch.athena.sql.rscache;

import java.util.Collections;
import java.util.List;

/**
 * Created by shaoyang.qi on 2017/9/13.
 * 此类用于记录QueryResultCache中与sql对应的缓存结果
 */
public class CacheRecord {
    public static final CacheRecord EMPTY = new CacheRecord(Collections.emptyList());
    public final List<byte[]> packets;
    public final long birthday;

    public CacheRecord(List<byte[]> packets) {
        this.packets = Collections.unmodifiableList(packets);
        this.birthday = System.currentTimeMillis();
    }

    public boolean isExpired(long expireTimeInMillis) {
        return (System.currentTimeMillis() - birthday) > expireTimeInMillis;
    }
}
