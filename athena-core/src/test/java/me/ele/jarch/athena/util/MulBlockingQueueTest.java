package me.ele.jarch.athena.util;

import me.ele.jarch.athena.util.schedule.DalQueueEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.BitSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class MulBlockingQueueTest {
    private static final Logger logger = LoggerFactory.getLogger(MulBlockingQueueTest.class);

    @Test public void poll() throws InterruptedException {
        MulBlockingQueue<Payload> mq = new MulBlockingQueue<>(1024);
        BitSet indexs = new BitSet(2);
        indexs.set(mq.register("eosgroup1"));
        indexs.set(mq.register("eosgroup2"));
        List<Payload> packets = mq.poll(indexs);
        Assert.assertTrue(packets.isEmpty());

        mq.put(new Payload(0, "p01"), 0);
        mq.put(new Payload(0, "p02"), 0);
        mq.put(new Payload(1, "p11"), 1);
        packets = mq.poll(indexs);
        Assert.assertEquals(packets.get(0).data, "p01");
        Assert.assertEquals(packets.get(1).data, "p11");
        packets = mq.poll(indexs);
        Assert.assertEquals(packets.get(0).data, "p02");

        mq.put(new Payload(0, "p03"), 0);
        indexs.clear(0);
        packets = mq.poll(indexs);
        Assert.assertTrue(packets.isEmpty());

        indexs.set(0);
        indexs.clear(1);
        mq.poll(indexs);
        mq.poll(indexs);
        mq.poll(indexs);
        long start = System.currentTimeMillis();
        indexs.set(0);
        mq.poll(200, TimeUnit.MILLISECONDS, indexs);
        long end = System.currentTimeMillis();
        Assert.assertTrue((end - start) >= 200);
    }

    @Test public void take() throws InterruptedException {
        final MulBlockingQueue<Payload> mq = new MulBlockingQueue<>(0x20);
        BitSet indexs = new BitSet(2);
        indexs.set(mq.register("eosgroup1"));
        indexs.set(mq.register("eosgroup2"));

        Thread a = new Thread(() -> {
            try {
                Thread.sleep(200);
                mq.put(new Payload(0, "package"), 0);
            } catch (Exception e) {
                logger.error("Exception in MulBlockingQueueTest.take():", e);
            }
        });
        long start = System.currentTimeMillis();
        a.start();
        mq.take(indexs);
        long end = System.currentTimeMillis();
        Assert.assertTrue((end - start) >= 200);
    }


    @Test public void takeMulThread() throws InterruptedException {
        final MulBlockingQueue<Payload> mq = new MulBlockingQueue<>(0x20);
        BitSet indexs = new BitSet(2);
        indexs.set(mq.register("eosgroup1"));
        indexs.set(mq.register("eosgroup2"));

        IntStream.range(0, 6).forEach(i -> {
            Thread t = new Thread(() -> {
                IntStream.range(0, 100000).forEach(time -> {
                    try {
                        mq.put(new Payload(0, "package"), 0);
                        mq.put(new Payload(1, "package"), 1);
                    } catch (Exception e) {
                        logger.error("Exception in MulBlockingQueueTest.takeMulThread():", e);
                    }

                });
            });
            t.start();
        });
        int count = 0;
        List<Payload> packets;
        while (count < 1200000) {
            packets = mq.take(indexs);
            count += packets.size();
        }
    }

    static class Payload extends DalQueueEntry implements Comparable<Payload> {
        final int id;
        final String data;

        private Payload(int queueId, String data) {
            this.id = queueId;
            this.data = data;
        }

        @Override public int compareTo(Payload o) {
            return 0;
        }

        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            Payload payload = (Payload) o;

            if (id != payload.id)
                return false;
            return data != null ? data.equals(payload.data) : payload.data == null;

        }

        @Override public int hashCode() {
            int result = id;
            result = 31 * result + (data != null ? data.hashCode() : 0);
            return result;
        }

        @Override public boolean isOverdraw() {
            return false;
        }
    }

    //    @Test
    public void pollLargeGroupsBenchMark() throws InterruptedException {
        MulBlockingQueue<Payload> mq = new MulBlockingQueue<>(102400);
        BitSet indexs = new BitSet(10240);
        for (int i = 0; i < 10000; i++) {
            indexs.set(mq.register("eosgroup" + i));
        }

        BitSet readyIds = mq.getReadyQueueIds();
        long start = System.currentTimeMillis();
        for (int i = 0; i < 1_000_000; i++) {
            mq.put(new Payload(0, "package"), 0);
            mq.put(new Payload(9999, "package"), 1);
            mq.poll(readyIds);
        }
        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }


    @Test public void getModCount() throws InterruptedException {
        MulBlockingQueue<Payload> mq = new MulBlockingQueue<>(1024);
        BitSet indexs = new BitSet(2);
        indexs.set(mq.register("eosgroup1"));
        indexs.set(mq.register("eosgroup2"));
        mq.poll(indexs);
        Assert.assertEquals(mq.getModCount(0), 0);
        Assert.assertEquals(mq.getModCount(1), 0);

        mq.put(new Payload(0, "p01"), 0);
        mq.put(new Payload(0, "p02"), 0);
        mq.put(new Payload(1, "p11"), 1);
        mq.poll(indexs);
        Assert.assertEquals(mq.getModCount(0), 3);
        Assert.assertEquals(mq.getModCount(1), 2);
        mq.poll(indexs);
        Assert.assertEquals(mq.getModCount(0), 4);
        Assert.assertEquals(mq.getModCount(1), 2);

        mq.put(new Payload(0, "p03"), 0);
        indexs.clear(0);
        mq.poll(indexs);
        Assert.assertEquals(mq.getModCount(0), 5);
        Assert.assertEquals(mq.getModCount(1), 2);

        indexs.set(0);
        mq.take(indexs);
        Assert.assertEquals(mq.getModCount(0), 6);
        Assert.assertEquals(mq.getModCount(1), 2);
    }
}
