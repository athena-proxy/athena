package me.ele.jarch.athena.scheduler;

import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Math.abs;

public class RequestThrottleTest {

    private FooExecutor fooExecutor;
    private RequestThrottle throttle;


    static class FooExecutor {
        public static final int PARALLEL_TASKS = 2;
        private static final ExecutorService executor =
            Executors.newFixedThreadPool(PARALLEL_TASKS);
        private AtomicLong counter = new AtomicLong(0);
        private volatile long windowBegin = -1;

        public FooExecutor() {
            windowBegin = System.currentTimeMillis();
        }

        public void execute(String payload, long workingCostInMillis) {
            executor.execute(() -> {
                try {
                    if (-1 == windowBegin) {
                    }
                    Thread.sleep(workingCostInMillis);
                    this.counter.incrementAndGet();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
        }

        public double getQPS() {
            long timeWindowInMillis = System.currentTimeMillis() - windowBegin;
            if (timeWindowInMillis <= 0) {
                timeWindowInMillis = 1;
            }

            // requests per second
            return counter.get() * 1000.0 / timeWindowInMillis;
        }
    }

    @BeforeTest public void beforeTest() throws InterruptedException {
        this.throttle = new RequestThrottle();
        this.fooExecutor = new FooExecutor();
    }

    @AfterTest public void afterTest() {
        this.fooExecutor = null;
        this.throttle = null;
    }

    @Test public void testEnqueue() throws InterruptedException {
        final int tasks = 100;
        for (int i = 0; i < tasks; ++i) {
            throttle.enqueuePayloadWithDelayInMillis("select 1", 0);
        }
        Thread.sleep(1000);
        System.out.println(throttle.getExecutedCount());
        Assert.assertEquals(throttle.getExecutedCount(), tasks);
    }

    /**
     * 测试case基于时间窗口，测试不稳定。在批量跑的场景下经常失败，所以禁止掉此case
     *
     * @throws InterruptedException
     */
    @Test(enabled = false) public void testNormalSpeed() throws InterruptedException {
        final long costInMills = 10;
        final long total_tasks = 2000;
        for (int i = 0; i < total_tasks; ++i) {
            this.fooExecutor.execute("cmd " + i, costInMills);
        }

        final double expectedQPS = 1000.0 / costInMills * FooExecutor.PARALLEL_TASKS;

        Thread.sleep((long) (total_tasks / expectedQPS * 1000));
        System.out.println("Actual QPS: " + fooExecutor.getQPS());
        System.out.println("Expected QPS: " + expectedQPS);

        Assert.assertTrue(abs(fooExecutor.getQPS() - expectedQPS) < expectedQPS * 0.3);

    }

    @Test(enabled = false) public void testThrottle() throws InterruptedException {
        RequestThrottle throttle = new RequestThrottle();

        final long costInMills = 10;
        final long delayInMills = 10;
        final long total_tasks = 2000;
        for (int i = 0; i < total_tasks; ++i) {
            throttle
                .enqueuePayloadWithDelayInMillis(() -> this.fooExecutor.execute("cmd", costInMills),
                    delayInMills);
        }

        final double expectedQPS = 1000.0 / costInMills * FooExecutor.PARALLEL_TASKS / 2;

        Thread.sleep((long) (total_tasks / expectedQPS * 1000));
        System.out.println("Actual QPS: " + fooExecutor.getQPS());
        System.out.println("Expected QPS: " + expectedQPS);

        Assert.assertTrue(abs(fooExecutor.getQPS() - expectedQPS) < expectedQPS * 0.3);

    }
}
