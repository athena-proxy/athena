package me.ele.jarch.athena.scheduler;


public class SchedulerTest {
    //
    //    static {
    //        LoggerFactory.getLogger("ROOT").setLevel(LogLevel.ERROR);
    //        AthenaConfig.getInstance().load();
    //    }
    //    private Scheduler secheduler = null;
    //
    //    @BeforeMethod
    //    public void setUp() throws Exception {
    //        this.secheduler = new Scheduler();
    //        this.secheduler.start();
    //    }
    //
    //    @AfterMethod
    //    public void Teardown() {
    //        Scheduler s = this.secheduler;
    //        this.secheduler = null;
    //        s.shutdown();
    //     }
    //
    //
    //
    //    /**
    //     *
    //     * @throws InterruptedException
    //     */
    //    @Test(timeOut=5000)
    //    public void testNormalExecuted() throws InterruptedException {
    //
    //        int size = 10_000;
    //        ZKCache.setMaxQueueSize(size);
    //        AtomicInteger count = new AtomicInteger();
    //        CountDownLatch latch = new CountDownLatch(size);
    //        long begin = System.currentTimeMillis();
    //        IntStream.range(0,  size).forEach( i -> this.secheduler.enqueue( new MockQuickSqlSessionContext( count, latch,this.secheduler)));
    //
    //        latch.await( 2, TimeUnit.SECONDS);
    //
    //        assertEquals(size, count.get());
    //
    //        System.out.println( "size: " + size + ", millisec: " + (System.currentTimeMillis() - begin));
    //
    //    }
    //
    //
    //    /**
    //     * 测试用例： 动态减小数据库并发数
    //     * 1. 设置数据库最大并发量为{@code size}
    //     * 2. 发送{@code size +2 }个慢请求（人为控制结束时间）
    //     * 3. 确认只有{@code size}个请求正在执行
    //     * 4. 设置数据库最大并发量为{@code size－2}（在另一个线程中执行，因为这个方法在得到信号量前会阻塞当前线程的执行）
    //     * 5. 设置所有请求执行完成
    //     * 6. 确认总执行数为{@code size+2}(已完成的也计算在内）
    //     * 7. 计数器清零
    //     * 8. 发送{@code size +2 }个慢请求（人为控制结束时间）
    //     * 9. 确认只有{@code size－2}个请求正在执行
    //     * 10. 设置一个请求为执行完成
    //     * 11. 确认只有{@code size －1}个请求正在执行
    //     * 12. 设置所有请求执行完成
    //     * 13. 确认总执行数为{@code size+2}(已完成的也计算在内）
    //     *
    //     * @throws InterruptedException
    //     */
    //
    //    @Test(timeOut = 10000)
    //    public void testChangeMaxDBSessionsSmaller() throws InterruptedException {
    //
    //        int size = 5;
    //        AtomicInteger count = new AtomicInteger();
    //
    //        MockSlowSqlSessionContext[] ctxArray = new MockSlowSqlSessionContext[size];
    //        IntStream.range(0, size).forEach(i -> ctxArray[i] = new MockSlowSqlSessionContext(count,this.secheduler));
    //
    //        // step 1, set max DB Session as 5
    //        this.secheduler.setMaxActiveDBSessions(size);
    //
    //        // step 2, send 5 requests
    //        IntStream.range(0, size).forEach(i -> this.secheduler.enqueue(ctxArray[i]));
    //
    //        Thread.sleep(10);
    //        // step 3, this request should be blocked and not executed.( count should be 5 )
    //        assertEquals(count.get(), size );
    //        IntStream.range(0, size).forEach(i -> ctxArray[i].releaseDbSemaphore());
    //
    //        Thread setter = new Thread() {
    //            @Override
    //            public void run() {
    //                secheduler.setMaxActiveDBSessions(size - 2);
    //            }
    //        };
    //        setter.start();
    //        setter.join();
    //
    //        secheduler.setMaxActiveDBSessions(size - 2);
    //
    //        IntStream.range(0, size).forEach(i -> ctxArray[i] = new MockSlowSqlSessionContext(count,this.secheduler));
    //
    //        // step 7
    //        boolean rel = count.compareAndSet(size, 0);
    //        assertTrue(rel);
    //
    //        // step 8
    //        for (int i = 0; i < size; ++i) {
    //            this.secheduler.enqueue(ctxArray[i]);
    //        }
    //        Thread.sleep(10);
    //
    //        // step 9
    //        assertEquals(size - 2, count.get());
    //
    //        // step 10
    //        ctxArray[0].releaseDbSemaphore();
    //        Thread.sleep(10);
    //
    //        // step 11
    //        assertEquals(size - 1, count.get());
    //
    //        // step 12
    //        IntStream.range(0, size).forEach(i -> ctxArray[i].releaseDbSemaphore());
    //        Thread.sleep(10);
    //
    //        // step 13
    //        assertEquals(size, count.get());
    //
    //    }
    //
    //    /**
    //     * 测试用例： 动态增大数据库并发数
    //     * 1. 设置数据库最大并发量为{@code size}
    //     * 2. 发送{@code size +2 }个慢请求（人为控制结束时间）
    //     * 3. 确认只有{@code size}个请求正在执行
    //     * 4. 设置数据库最大并发量为{@code size+1}
    //     * 5. 确认有{@code size＋1}个请求正在执行
    //     * 6. 设置一个请求为执行完成
    //     * 7. 确认总执行数为{@code size+2}(已完成的也计算在内）
    //     * 8. 设置所有请求执行完成
    //     *
    //     * @throws InterruptedException
    //     */
    //    @Test(timeOut=20000)
    //    public void testChangeMaxDBSessionsLarger() throws InterruptedException {
    //
    //        int size = 5;
    //        AtomicInteger count = new AtomicInteger();
    //
    //        MockSlowSqlSessionContext[] ctxArray = new MockSlowSqlSessionContext[size+2];
    //        IntStream.range(0, size+2).forEach(i -> ctxArray[i] = new MockSlowSqlSessionContext(count,this.secheduler));
    //
    //        // step 1, set max DB Session as 5
    //        this.secheduler.setMaxActiveDBSessions(size);
    //
    //        // step 2, send 7 requests
    //        //IntStream.range(0, size+2).forEach(i -> this.secheduler.enqueue( ctxArray[i]));
    //        for ( int i = 0; i < size +2; ++i ) {
    //            this.secheduler.enqueue( ctxArray[i]);
    //        }
    //
    //        Thread.sleep(10);
    //        // step 3, the 5 requests should be executed.
    //        assertEquals(size, count.get());
    //
    //
    //        // step 4
    //         secheduler.setMaxActiveDBSessions(size + 1);
    //
    //
    //        Thread.sleep(10);
    //
    //        // step 5, the blocking request should be exeucted. ( count should be 6 )
    //        assertEquals(size+1, count.get());
    //
    //        // step 6
    //        ctxArray[0].releaseDbSemaphore();
    //        Thread.sleep(10);
    //
    //        // step 7
    //        assertEquals(size+2, count.get());
    //
    //
    //        // step 8, release all sessions so the scheduler thread pool will be shutdown quickly.
    //        IntStream.range(0, size+1).forEach(i -> ctxArray[i].releaseDbSemaphore());
    //
    //    }
    //
    //
    //    static class  MockQuickSqlSessionContext extends SqlSessionContext {
    //        public final AtomicInteger executedCount;
    //        public final CountDownLatch latch;
    //        public MockQuickSqlSessionContext( AtomicInteger executedCount, CountDownLatch latch,Scheduler scheduler) {
    //            super(scheduler);
    //            this.executedCount = executedCount;
    //            this.latch = latch;
    //        }
    //
    //        @Override
    //        protected boolean doInnerExecute() {
    //             executedCount.incrementAndGet();
    //            latch.countDown();
    //            this.releaseDbSemaphore();
    //            return true;
    //        }
    //    }
    //
    //    static class MockSlowSqlSessionContext extends SqlSessionContext {
    //        public final AtomicInteger executedCount;
    //
    //        public MockSlowSqlSessionContext( AtomicInteger executedCount,Scheduler scheduler) {
    //            super(scheduler);
    //            this.executedCount = executedCount;
    //        }
    //
    //        @Override
    //        public void execute() {
    //            this.doInnerExecute();
    //        }
    //
    //        @Override
    //        protected boolean doInnerExecute() {
    //            executedCount.incrementAndGet();
    //            return true;
    //        }
    //    }

}
