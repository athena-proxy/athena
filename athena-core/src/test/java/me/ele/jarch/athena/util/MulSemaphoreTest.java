package me.ele.jarch.athena.util;

import me.ele.jarch.athena.util.MulSemaphore.DALPermitsSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public class MulSemaphoreTest {
    private static final Logger logger = LoggerFactory.getLogger(MulSemaphoreTest.class);

    //    @Test
    public void benchmark1() {
        final BitSet bitset = new BitSet(102400);
        final AtomicInteger count = new AtomicInteger();
        for (int i = 0; i < 1024; i++) {
            bitset.set(i);
        }
        long start = System.currentTimeMillis();
        for (int i = 0; i < 100_000; i++) {
            for (int id = bitset.nextSetBit(0); id >= 0; id = bitset.nextSetBit(id + 1)) {
                count.getAndIncrement();
            }
        }
        System.out.println("benchmark1 " + (System.currentTimeMillis() - start));
        Assert.assertEquals(count.get(), 102400_000);
    }

    //    @Test
    public void benchmark2() {
        final LinkedHashMap<Integer, Object> hashMap = new LinkedHashMap<>(102400);
        final AtomicInteger count = new AtomicInteger();
        for (int i = 0; i < 1024; i++) {
            hashMap.put(i, "");
        }
        long start = System.currentTimeMillis();
        for (int i = 0; i < 100_000; i++) {
            hashMap.forEach((id, value) -> {
                count.getAndIncrement();
            });
        }
        System.out.println("benchmark2 " + (System.currentTimeMillis() - start));
        Assert.assertEquals(count.get(), 102400_000);
    }


    @Test public void acquire() throws InterruptedException {
        final MulSemaphore ms = new MulSemaphore(0x20);
        BitSet bitset = new BitSet(0x20);
        bitset.set(0, 2);
        ms.putPermits(0, 10, 0);
        ms.putPermits(1, 10, 0);

        Assert.assertEquals(ms.availablePermits(0), 10);
        final List<DALPermitsSet> dalPermits = new ArrayList<>();
        IntStream.range(0, 10).forEach(i -> {
            try {
                dalPermits.add(ms.acquire(bitset));
            } catch (Exception e) {
                logger.error("Exception in MulSemaphoreTest.acquire():", e);
            }
        });
        Assert.assertEquals(dalPermits.size(), 10);
        Thread t = new Thread(() -> {
            try {
                Thread.sleep(200);
                dalPermits.forEach(dalp -> {
                    dalp.release();
                });
            } catch (Exception e) {
                logger.error("Exception in MulSemaphoreTest.acquire():", e);
            }
        });
        long start = System.currentTimeMillis();
        t.start();
        ms.acquire(bitset);
        long dur = System.currentTimeMillis() - start;
        Assert.assertTrue(dur >= 200);
        t.join();
        Assert.assertEquals(ms.availablePermits(0), 9);

    }

    @Test public void tryAcquire() throws InterruptedException {
        final MulSemaphore ms = new MulSemaphore(0x20);
        BitSet bitset = new BitSet(0x20);
        bitset.set(0, 2);
        ms.putPermits(0, 1, 0);
        ms.putPermits(1, 1, 0);
        Assert.assertEquals(ms.availablePermits(0), 1);

        final List<DALPermitsSet> dalPermits = new ArrayList<>();
        dalPermits.add(ms.tryAcquire(bitset));
        Assert.assertEquals(dalPermits.size(), 1);

        long start = System.currentTimeMillis();
        Assert.assertTrue(ms.tryAcquire(20, TimeUnit.MILLISECONDS, bitset).getIds().isEmpty());
        long dur = System.currentTimeMillis() - start;
        Assert.assertTrue(dur >= 20);

    }

    @Test public void acquireMulThread() throws InterruptedException {
        final MulSemaphore ms = new MulSemaphore(1024);
        BitSet bitset = new BitSet(1024);
        bitset.set(0, 3);
        ms.putPermits(0, 20, 0);
        ms.putPermits(1, 20, 0);
        ms.putPermits(2, 20, 0);
        Assert.assertEquals(ms.availablePermits(0), 20);
        Assert.assertEquals(ms.availablePermits(1), 20);
        Assert.assertEquals(ms.availablePermits(2), 20);
        final List<Thread> ts = new ArrayList<>();
        IntStream.range(0, 40).forEach(i -> {
            Thread t = new Thread(() -> {
                try {
                    for (int time = 0; time < 100000; time++) {
                        DALPermitsSet set = ms.acquire(bitset);
                        set.release();
                    }
                } catch (Exception e) {
                    logger.error("Exception in MulSemaphoreTest.acquireMulThread():", e);
                }
            });
            ts.add(t);
            t.start();
        });
        for (Thread t : ts) {
            t.join();
        }
        Assert.assertEquals(ms.availablePermits(0), 20);
        Assert.assertEquals(ms.availablePermits(1), 20);
        Assert.assertEquals(ms.availablePermits(2), 20);

    }
}
