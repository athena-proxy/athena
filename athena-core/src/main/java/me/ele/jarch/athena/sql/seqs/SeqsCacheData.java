package me.ele.jarch.athena.sql.seqs;

import me.ele.jarch.athena.allinone.DBConnectionInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class SeqsCacheData {
    private volatile long seq_id_Base;
    private final AtomicLong seqID = new AtomicLong(0);
    private volatile int cache_size = 0;
    private volatile DBConnectionInfo seedSource = new DBConnectionInfo();

    public SeqsCacheData() {
    }

    synchronized public void appendSeqSeed(long seq_id_Base, int cache_size,
        DBConnectionInfo seedSource) {
        this.seq_id_Base = seq_id_Base;
        this.seqID.set(0);
        this.cache_size = cache_size;
        this.seedSource = seedSource;
    }

    /**
     * if range == 1 return value
     * or return start, end
     *
     * @param range
     * @return
     */
    synchronized public List<Long> getNextSeqValue(long range) {

        List<Long> result = new ArrayList<>();
        if (seqID.get() + range - 1 < cache_size) {
            if (range == 1) {
                result.add(seqID.incrementAndGet() + seq_id_Base);
                return result;
            } else {
                result.add(seqID.incrementAndGet() + seq_id_Base);
                result.add(seqID.addAndGet(range - 1) + seq_id_Base);
                return result;
            }
        }
        return null;
    }

    public DBConnectionInfo getSeedSource() {
        return seedSource;
    }

    public long getSeq_id_Base() {
        return seq_id_Base;
    }

    public long getSeqID() {
        return seqID.get();
    }

    public int getCache_size() {
        return cache_size;
    }
}
