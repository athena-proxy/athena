package me.ele.jarch.athena.util;

import com.google.common.util.concurrent.AtomicDouble;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.BitSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.IntStream;

public class MulSemaphore {
    //共享时间片的单个信号量的轮转时间
    private static final long DECIMAL_SEMAPHORE_SHARE_TIME_PERIOD = TimeUnit.SECONDS.toMillis(1);
    //信号量延迟归还任务调度器
    private static final JobScheduler SEMAPHORE_DELAY_RETURNED_JOB_SCHEDULER =
        new JobScheduler("SEMAPHORE_DELAY_RETURNED_JOB_SCHEDULER ");

    static {
        SEMAPHORE_DELAY_RETURNED_JOB_SCHEDULER.start();
    }

    //由于会大量使用DALPermitsSet,为减轻GC压力,使用缓存重用DALPermitsSet,此处缓存实现为ConcurrentLinkedQueue
    private final ConcurrentLinkedQueue<DALPermitsSet> dalPermitsSetPool =
        new ConcurrentLinkedQueue<>();
    private final AtomicInteger poolSize = new AtomicInteger(0);
    private final static int MAXPoolSize = 100;
    private static final Logger LOGGER = LoggerFactory.getLogger(MulSemaphore.class);
    private final int MAXSemaphoreNumber;
    private final static int MAX_OVERDRAW = 2;
    /**
     * Lock held by take
     */
    private final ReentrantLock takeLock = new ReentrantLock();
    /**
     * Wait queue for waiting takes
     */
    private final Condition notEmpty = takeLock.newCondition();

    private final AtomicInteger[] semaphores;
    //存储小数信号量扩大1000倍的值，0.5->0.5*1000=500
    private final AtomicInteger[] decimalSemaphores;
    //小数信号量延迟归还的时间倍数
    private final AtomicDouble[] decimalSemaphoreDelayReturnedTimeMultis;
    //小数部分信号量是否被使用的标记。
    private final AtomicBoolean[] decimalSemaphoreUsedMarks;

    public MulSemaphore(int maxSemaphoreNumber) {
        this.MAXSemaphoreNumber = maxSemaphoreNumber;
        this.semaphores = new AtomicInteger[MAXSemaphoreNumber];
        this.decimalSemaphores = new AtomicInteger[MAXSemaphoreNumber];
        this.decimalSemaphoreDelayReturnedTimeMultis = new AtomicDouble[MAXSemaphoreNumber];
        this.decimalSemaphoreUsedMarks = new AtomicBoolean[MAXSemaphoreNumber];
        IntStream.range(0, MAXSemaphoreNumber).forEach(i -> {
            this.semaphores[i] = new AtomicInteger();
            this.decimalSemaphores[i] = new AtomicInteger();
            this.decimalSemaphoreDelayReturnedTimeMultis[i] = new AtomicDouble(0.0);
            this.decimalSemaphoreUsedMarks[i] = new AtomicBoolean(false);
        });
    }

    synchronized public void unregister(int id) {
        this.semaphores[id] = new AtomicInteger();
    }

    public int availablePermits(int id) {
        return semaphores[id].get();
    }

    private boolean hasAvailablePermits(BitSet ids) {
        for (int id = ids.nextSetBit(0); id >= 0; id = ids.nextSetBit(id + 1)) {
            if (semaphores[id].get() > 0) {
                return true;
            }
        }
        return false;
    }

    public void putPermits(int id, int permits, int decimalPermits) {
        AtomicInteger semaphore = semaphores[id];
        int c = semaphore.getAndAdd(permits);

        AtomicInteger decimalSemaphore = decimalSemaphores[id];
        int oldDecimalSemaphore = decimalSemaphore.get();
        int newDecimalSemaphore = decimalSemaphore.addAndGet(decimalPermits);

        double delayReturnedTimeMulti = 0.0;
        if (newDecimalSemaphore != 0) {
            delayReturnedTimeMulti =
                (DECIMAL_SEMAPHORE_SHARE_TIME_PERIOD - newDecimalSemaphore) * 1.0
                    / newDecimalSemaphore;
        }
        this.decimalSemaphoreDelayReturnedTimeMultis[id].set(delayReturnedTimeMulti);

        if (newDecimalSemaphore > oldDecimalSemaphore && oldDecimalSemaphore == 0) {
            semaphore.getAndIncrement();
        }
        if (newDecimalSemaphore < oldDecimalSemaphore && newDecimalSemaphore == 0) {
            semaphore.getAndDecrement();
        }

        if (c <= 0) {
            signalNotEmpty();
        }
    }


    /**
     * @param semaphore
     * @param permits
     */
    private void putPermits(AtomicInteger semaphore, int permits) {
        int c = semaphore.getAndAdd(permits);
        if (c <= 0) {
            signalNotEmpty();
        }
    }

    public DALPermitsSet tryAcquire(BitSet ids) throws InterruptedException {
        return doTryAcquire(ids, 0);
    }

    public DALPermitsSet overdrawAcquire(BitSet ids) throws InterruptedException {
        return doTryAcquire(ids, MAX_OVERDRAW);
    }

    private DALPermitsSet doTryAcquire(BitSet ids, int overdraw) throws InterruptedException {
        DALPermitsSet permitsSet = newDALPermitsSet();
        for (int i = ids.nextSetBit(0); i >= 0; i = ids.nextSetBit(i + 1)) {
            AtomicInteger semaphore = semaphores[i];
            int remain = semaphore.getAndDecrement();
            //虚拟剩余的信号量=实际剩余信号量+最大透支信号量
            int virtualRemain = remain + overdraw;
            if (virtualRemain > 1) {
                //搜寻有消息且有信号量执行的阻塞队列关联的信号量许可
                DALPermits x = new DALPermits(i, 1, semaphore);
                permitsSet.put(x);
                continue;
            }
            if (virtualRemain < 1) {
                //搜寻有消息但无信号量执行的阻塞队列关联的信号量许可
                permitsSet.setMisId(i);
                semaphore.incrementAndGet();
                continue;
            }
            //最后一个信号量可能是共享时间片信号量
            int decimalSemaphore = decimalSemaphores[i].get();
            AtomicBoolean decimalSemaphoreUsedMark = decimalSemaphoreUsedMarks[i];
            if (decimalSemaphore != 0 && decimalSemaphoreUsedMark.compareAndSet(false, true)) {
                DALPermits x = new DALPermits(i, 1, semaphore, true);
                permitsSet.put(x);
                continue;
            }
            DALPermits x = new DALPermits(i, 1, semaphore);
            permitsSet.put(x);
        }
        return permitsSet;
    }

    public DALPermitsSet tryAcquire(long time, TimeUnit timeUnit, BitSet ids)
        throws InterruptedException {

        DALPermitsSet re = tryAcquire(ids);
        if (re.getMisIds().isEmpty() || !re.getIds().isEmpty()) {
            return re;
        }
        long nanos = timeUnit.toNanos(time);
        final ReentrantLock takeLock = this.takeLock;
        takeLock.lockInterruptibly();
        try {
            while (!hasAvailablePermits(re.getMisIds())) {
                if (nanos <= 0) {
                    re.release();
                    return newDALPermitsSet();
                }
                nanos = notEmpty.awaitNanos(nanos);
                signalNotEmpty();
            }
        } finally {
            takeLock.unlock();
        }
        DALPermitsSet tmp = re;
        re = tryAcquire(tmp.getMisIds());
        tmp.release();
        return re;
    }

    public DALPermitsSet acquire(BitSet ids) throws InterruptedException {
        final ReentrantLock takeLock = this.takeLock;
        DALPermitsSet re = tryAcquire(ids);
        if (re.getMisIds().isEmpty()) {
            return re;
        }
        while (re.getIds().isEmpty()) {
            takeLock.lockInterruptibly();
            try {
                while (!hasAvailablePermits(re.getMisIds())) {
                    notEmpty.await();
                    signalNotEmpty();
                }
            } finally {
                takeLock.unlock();
            }
            DALPermitsSet tmp = re;
            re = tryAcquire(re.getMisIds());
            tmp.release();
        }
        return re;
    }

    /**
     * Signals a waiting take. Called only from putPermits (which do not otherwise ordinarily lock takeLock.)
     */
    private void signalNotEmpty() {
        final ReentrantLock takeLock = this.takeLock;
        takeLock.lock();
        try {
            notEmpty.signal();
        } finally {
            takeLock.unlock();
        }
    }

    public DALPermitsSet newDALPermitsSet() {
        DALPermitsSet instance = dalPermitsSetPool.poll();
        if (instance == null) {
            instance = new DALPermitsSet();
            LOGGER.info("new DALPermitsSet");
        } else {
            poolSize.getAndDecrement();
        }
        return instance;
    }

    public class DALPermitsSet {
        private final DALPermits[] permitsList = new DALPermits[MAXSemaphoreNumber];
        private final BitSet ids = new BitSet(MAXSemaphoreNumber);
        //lack of semaphore blocking queue ids
        //有消息但是无信号量执行的阻塞队列ids
        private final BitSet misids = new BitSet(MAXSemaphoreNumber);

        public void release() {
            for (int i = ids.nextSetBit(0); i >= 0; i = ids.nextSetBit(i + 1)) {
                permitsList[i].release();
                permitsList[i] = null;
            }
            ids.clear();
            misids.clear();
            if (poolSize.get() < MAXPoolSize) {
                dalPermitsSetPool.offer(this);
                poolSize.getAndIncrement();
            } else {
                LOGGER.error("DALPermitsSet poll leak");
            }
        }

        public void merge(DALPermitsSet dalPermitsSet) {
            for (int id = dalPermitsSet.getMisIds().nextSetBit(0);
                 id >= 0; id = dalPermitsSet.getMisIds().nextSetBit(id + 1)) {
                this.setMisId(id);
            }
            for (int id = dalPermitsSet.getIds().nextSetBit(0);
                 id >= 0; id = dalPermitsSet.getIds().nextSetBit(id + 1)) {
                this.put(dalPermitsSet.take(id));
            }
        }

        public BitSet getMisIds() {
            return misids;
        }

        public BitSet getIds() {
            return ids;
        }

        public DALPermits take(int id) {
            DALPermits dalPermits = null;
            if (ids.get(id)) {
                ids.clear(id);
                dalPermits = permitsList[id];
                permitsList[id] = null;
            }
            return dalPermits;
        }

        public void put(DALPermits permits) {
            if (ids.get(permits.id)) {
                this.take(permits.id).release();
                LOGGER.warn("DALpermits overlap");
            }
            ids.set(permits.id);
            misids.clear(permits.id);
            permitsList[permits.id] = permits;
        }

        public void setMisId(int id) {
            this.misids.set(id);
        }
    }


    public class DALPermits {
        final public int id;
        final public int permits;
        final private AtomicInteger semaphore;
        final private AtomicBoolean active = new AtomicBoolean(true);
        final private boolean decimalSemaphoreUsedMark;
        final private long birthday;

        private DALPermits(int id, int permits, AtomicInteger semaphore) {
            this(id, permits, semaphore, false);
        }

        private DALPermits(int id, int permits, AtomicInteger semaphore,
            boolean decimalSemaphoreUsedMark) {
            this.id = id;
            this.permits = permits;
            this.semaphore = semaphore;
            this.decimalSemaphoreUsedMark = decimalSemaphoreUsedMark;
            this.birthday = System.currentTimeMillis();
        }

        private void realRelease() {
            if (!active.getAndSet(false)) {
                return;
            }
            if (decimalSemaphoreUsedMark) {
                decimalSemaphoreUsedMarks[id].set(false);
            }
            putPermits(semaphore, permits);
        }

        public void release() {
            if (!decimalSemaphoreUsedMark) {
                realRelease();
                return;
            }
            long execTimeInMillis = System.currentTimeMillis() - birthday;
            if (execTimeInMillis <= 0) {
                realRelease();
                return;
            }
            double expandDelayTimes = decimalSemaphoreDelayReturnedTimeMultis[id].get();
            long delayTimeInMillis = (long) (expandDelayTimes * execTimeInMillis);
            delayTimeInMillis = Math.min(delayTimeInMillis, DECIMAL_SEMAPHORE_SHARE_TIME_PERIOD);
            SEMAPHORE_DELAY_RETURNED_JOB_SCHEDULER
                .addOneTimeJob("semaphore delay returned", delayTimeInMillis, this::realRelease);
        }
    }

    @Override public String toString() {
        return "MulSemaphore [semaphores=" + Arrays.toString(semaphores) + "]";
    }

    public String getInfo(BitSet ids) {
        final StringBuilder sb = new StringBuilder();
        ids.stream().forEach(id -> {
            sb.append(id + ":");
            sb.append(semaphores[id].get());
            sb.append(",");
        });
        return "MulSemaphore [semaphores=" + sb.toString() + "]";
    }

}
