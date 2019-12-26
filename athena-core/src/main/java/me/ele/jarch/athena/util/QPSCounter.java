package me.ele.jarch.athena.util;

import me.ele.jarch.athena.scheduler.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

public class QPSCounter {
    private static final Logger logger = LoggerFactory.getLogger(QPSCounter.class);
    private static final int SMALL_PERIOD = 100;

    // 记录1分钟内的QPS
    private static final QPSCounter QPS_COUNTER = new QPSCounter(60 * 1000 / SMALL_PERIOD);

    public static QPSCounter getInstance() {
        return QPS_COUNTER;
    }

    private final WindowQPS[] list;
    private final AtomicLong[] sleepCounters;
    private final AtomicLong[] semaphoreCounters;
    private final int size;
    private int curPos = 0;


    public static class WindowQPS {
        private final ConcurrentHashMap<Integer, AtomicInteger> qps = new ConcurrentHashMap<>();
        private final long from;
        private long to;

        private WindowQPS(long from) {
            this.from = from;
        }

        @Override public String toString() {
            if (this == EMPTY_QPS) {
                return "EMPTY_WindowQPS";
            }
            return "WindowQPS [qps=" + qps + ", from=" + from + ", to=" + to + "]";
        }

        public static final WindowQPS EMPTY_QPS = new WindowQPS(0);
    }

    public QPSCounter(int length) {
        this.size = length;
        this.list = new WindowQPS[size];
        final int multiSchedSize = AthenaConfig.getInstance().getMulschedulerCapacity();
        this.sleepCounters = new AtomicLong[multiSchedSize];
        this.semaphoreCounters = new AtomicLong[multiSchedSize];
        IntStream.range(0, size).forEach(i -> {
            list[i] = WindowQPS.EMPTY_QPS;
        });
        IntStream.range(0, multiSchedSize).forEach(i -> {
            this.sleepCounters[i] = new AtomicLong(0);
            this.semaphoreCounters[i] = new AtomicLong(0);
        });
        windowQPS = new WindowQPS(System.currentTimeMillis());
        list[curPos] = windowQPS;
    }

    private volatile long last = System.currentTimeMillis();
    private volatile WindowQPS windowQPS;

    private void appendOnePeriodQPS() {
        long now = System.currentTimeMillis();
        if (now - last > SMALL_PERIOD) {
            synchronized (this) {
                if (now - last > SMALL_PERIOD) {
                    WindowQPS _qps = windowQPS;
                    windowQPS = new WindowQPS(now);
                    curPos++;
                    if (curPos >= size) {
                        curPos = 0;
                    }
                    _qps.to = now;
                    list[curPos] = _qps;
                    last = System.currentTimeMillis();
                }
            }
        }
    }

    private static AtomicInteger f(AtomicInteger i) {
        i.incrementAndGet();
        return i;
    }

    private void incrQPS(int schedId) {
        appendOnePeriodQPS();
        windowQPS.qps.compute(schedId, (k, v) -> (v == null) ? new AtomicInteger(1) : f(v));
        sleepCounters[schedId].incrementAndGet();
        semaphoreCounters[schedId].incrementAndGet();
    }

    // QPS+1
    public void incrQPS(Scheduler sched) {
        incrQPS(sched.getDbQueueId());
    }

    private static final AtomicInteger ZERO = new AtomicInteger();

    // 获取前fetchPeriod毫秒的QPS
    public int getQPS(int schedId, int fetchPeriod) {
        if (fetchPeriod <= 0) {
            return -1;
        }
        appendOnePeriodQPS();
        final int _curPos = curPos;
        int index = _curPos;
        int rt = windowQPS.qps.getOrDefault(schedId, ZERO).get();
        long range_to = System.currentTimeMillis();
        long range_from = range_to - fetchPeriod;
        do {
            WindowQPS _qps = list[index];
            if (_qps.from >= range_from && _qps.to <= range_to) {
                rt += _qps.qps.getOrDefault(schedId, ZERO).get();
            } else {
                return rt;
            }
        } while (--index >= 0);
        index = size - 1;
        do {
            WindowQPS _qps = list[index];
            if (_qps.from >= range_from && _qps.to <= range_to) {
                rt += _qps.qps.getOrDefault(schedId, ZERO).get();
            } else {
                return rt;
            }
        } while (--index >= _curPos);
        return rt;
    }

    public long getSleepQps(int schedId) {
        return sleepCounters[schedId].getAndSet(0);
    }

    public long getSemaphoreQps(int schedId) {
        return semaphoreCounters[schedId].getAndSet(0);
    }

    @Override public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("QPSCounter [list=,\n");
        for (WindowQPS qps : list) {
            sb.append(qps.toString()).append(",\n");
        }
        sb.append(", size=" + size + ", curPos=" + curPos + ", last=" + last + ", curr_windowQPS="
            + windowQPS + "]");
        return sb.toString();
    }

    public static void main(String[] args) {
        QPSCounter counter = new QPSCounter(10);
        Random r = new Random();
        IntStream.range(0, 200).forEach(i -> {
            counter.incrQPS(3);
            try {
                Thread.sleep(r.nextInt(5));
            } catch (Exception e) {
                logger.error(Objects.toString(e));
                // e.printStackTrace();
            }
        });
        counter.incrQPS(3);
        counter.incrQPS(3);
        counter.incrQPS(3);
        System.out.println(counter.toString());
        System.out.println(System.currentTimeMillis());
        System.out.println(counter.getQPS(3, 80));
    }
}
