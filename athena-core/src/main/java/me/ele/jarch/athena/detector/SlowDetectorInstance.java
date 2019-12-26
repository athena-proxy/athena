package me.ele.jarch.athena.detector;

import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.util.rmq.SlowSqlInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Dal-Dev-Team on 17/3/9.
 */
public class SlowDetectorInstance implements Detector, SlowSqlProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(SlowDetectorInstance.class);

    private final SlowDetector DUMMY = new SlowDetector() {
        @Override public void mergeSQLSampleIfAbsent(SlowDetector prev) {
        }

        @Override public void calc() {
        }

        @Override public void addSample(long dur, SqlSessionContext ctx) {
        }
    };

    public SlowDetector prevDetector = DUMMY;

    public SlowDetector curDetector = DUMMY;

    private final LinkedBlockingQueue<SlowSqlInfo> slowSqlInfos = new LinkedBlockingQueue<>(1024);

    @Override public Object statistics() {
        prevDetector.calc();
        return prevDetector;
    }

    @Override public void startDetect() {
        SlowDetector cur = curDetector;
        curDetector = new SlowDetector();
        cur.endDetect();
        if (cur.groupSamples.isEmpty() && cur.sqlSamples.isEmpty()) {
            return;
        }
        try {
            cur.detectWithPrev(prevDetector, slowSqlInfos);
            cur.mergeSQLSampleIfAbsent(prevDetector);
        } catch (Exception e) {
            LOGGER.error("", e);
        }
        prevDetector = cur;
    }

    @Override public void addSample(long dur, SqlSessionContext ctx) {
        curDetector.addSample(dur, ctx);
    }

    @Override public SlowSqlInfo pollSlowSql() {
        return slowSqlInfos.poll();
    }
}
