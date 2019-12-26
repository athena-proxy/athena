package me.ele.jarch.athena.detector;

import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.util.NoThrow;
import me.ele.jarch.athena.util.rmq.SlowSqlInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Dal-Dev-Team on 17/3/9.
 */
public class DetectorDelegate {
    private static final Logger LOGGER = LoggerFactory.getLogger(DetectorDelegate.class);

    private static final Detector DUMMY = new Detector() {
        @Override public Object statistics() {
            return "Slow Detector is turn off";
        }

        @Override public void startDetect() {
        }

        @Override public void addSample(long dur, SqlSessionContext ctx) {
        }
    };

    public static void startDetect() {
        NoThrow.call(() -> activeDetector.startDetect());
    }

    public static void addSample(long dur, SqlSessionContext ctx) {
        NoThrow.call(() -> activeDetector.addSample(dur, ctx));
    }

    public static Object statistics() {
        try {
            return activeDetector.statistics();
        } catch (Exception e) {
            return e;
        }
    }

    // 当前正在使用的Detector
    private static volatile Detector activeDetector = DUMMY;

    private static boolean isTurnOn = false;

    public synchronized static void turnOn(boolean on) {
        if (on) {
            isTurnOn = true;
            activeDetector = new SlowDetectorInstance();
            LOGGER.info("Turn on the slow detector");
        } else {
            isTurnOn = false;
            activeDetector = DUMMY;
            LOGGER.info("Turn off the slow detector");
        }
    }

    public static SlowSqlInfo getSlowSqlInfo() {
        if (!isTurnOn || activeDetector == DUMMY) {
            return null;
        }
        if (activeDetector instanceof SlowSqlProducer) {
            SlowSqlProducer producer = (SlowSqlProducer) activeDetector;
            return producer.pollSlowSql();
        }
        return null;
    }
}
