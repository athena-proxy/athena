package me.ele.jarch.athena.scheduler;

import com.github.mpjct.jmpjct.util.ErrorCode;
import me.ele.jarch.athena.allinone.DBRole;
import me.ele.jarch.athena.constant.Metrics;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.util.AthenaConfig;
import me.ele.jarch.athena.util.MulBlockingQueue;
import me.ele.jarch.athena.util.MulSemaphore;
import me.ele.jarch.athena.util.MulSemaphore.DALPermits;
import me.ele.jarch.athena.util.MulSemaphore.DALPermitsSet;
import me.ele.jarch.athena.util.etrace.MetricFactory;
import me.ele.jarch.athena.util.schedule.DalQueueEntry;
import me.ele.jarch.athena.worker.SchedulerWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class MulScheduler {
    private static final Logger LOGGER = LoggerFactory.getLogger(MulScheduler.class);
    private final int workerCount;
    private final int capacity;

    private final MulBlockingQueue<Payload> mulQueue;
    private final MulSemaphore mulSemaphore;
    private final Scheduler[] registeredScheduler;
    private final BitSet registeredIds;
    private final List<Worker> workers = new ArrayList<>();


    private static class LazyHolder {
        private static final MulScheduler INSTANCE =
            new MulScheduler(AthenaConfig.getInstance().getMulschedulerCapacity(),
                AthenaConfig.getInstance().getMulschedulerWorkerCount());

        static {
            INSTANCE.start();
        }
    }

    public static MulScheduler getInstance() {
        return LazyHolder.INSTANCE;
    }

    private MulScheduler(int mulselectorCapacity, int mulselectorWorkerCount) {
        workerCount = mulselectorWorkerCount;
        capacity = mulselectorCapacity;
        mulQueue = new MulBlockingQueue<>(capacity);
        mulSemaphore = new MulSemaphore(capacity);
        registeredScheduler = new Scheduler[capacity];
        registeredIds = new BitSet(capacity);
        IntStream.range(0, workerCount).forEach(i -> {
            Worker worker = new Worker();
            worker.setName("MulSelectorWorker-" + i);
            workers.add(worker);
        });
    }

    public void start() {
        workers.forEach(worker -> {
            worker.start();
        });
    }

    synchronized public int register(Scheduler scheduler, String name, int permits, int decimal) {
        int id = mulQueue.register(name);
        registeredIds.set(id);
        registeredScheduler[id] = scheduler;
        mulSemaphore.putPermits(id, permits, decimal);
        LOGGER
            .warn(scheduler.getInfo().getQualifiedDbId() + " name:" + name + " register ID:" + id);
        return id;
    }

    synchronized public void unregister(int id, Scheduler scheduler) {
        if (registeredScheduler[id] != scheduler) {
            LOGGER.error("unregisted wrong id new:{}, old:{}", registeredScheduler[id], scheduler);
            return;
        }
        registeredIds.clear(id);
        mulQueue.unregister(id);
        mulSemaphore.unregister(id);
        registeredScheduler[id] = null;
        LOGGER.warn(scheduler.getInfo().getQualifiedDbId() + " unregister ID:" + id);
    }

    public boolean enqueue(SqlSessionContext sqlCtx, int id, PayloadType type, long seq,
        int priority, boolean isOverdraw) {
        try {
            this.mulQueue.put(new Payload(sqlCtx, id, type, seq, priority, isOverdraw), id);
            return true;
        } catch (InterruptedException e) {
            LOGGER.error(Objects.toString(e));
            return false;
        }
    }

    /**
     * Change the MaxActiveDBSessions
     *
     * @param id
     * @param delta
     * @param decimal 小数位信号量的1000倍
     *                e.g. 1, -100 :  1 + -100/1000 = 0.9  increase 0.9
     *                -1, 300  :  -1 + 300/1000 = -0.6 decrease 0.6
     */
    public synchronized void changeMaxSemaphore(int id, int delta, int decimal) {
        this.mulSemaphore.putPermits(id, delta, decimal);
    }

    public int getSemaphoreAvailablePermits(int id) {
        return mulSemaphore.availablePermits(id);
    }

    public int getQueueBlockingTaskCount(int id) {
        return mulQueue.size(id);
    }

    public void close() {
        workers.forEach(worker -> {
            worker.interrupt();
        });
    }

    public int queueSize(int selectorId) {
        return this.mulQueue.size(selectorId);
    }

    /**
     * @return 返回当前已经使用的槽位数
     */
    public int size() {
        return mulQueue.queueSize();
    }

    /**
     * @return 返回MulScheduler所能容纳的最大槽位数
     */
    public int capacity() {
        return capacity;
    }

    static class Payload extends DalQueueEntry implements Comparable<Payload> {
        final int id;
        final PayloadType type;
        final SqlSessionContext sqlCtx;
        final long seq;
        final int priority;
        final boolean isOverdraw;

        private Payload(SqlSessionContext sqlCtx, int queueId, PayloadType type, long seq,
            int priority, boolean isOverdraw) {
            this.id = queueId;
            this.sqlCtx = sqlCtx;
            this.type = type;
            this.seq = seq;
            this.priority = priority;
            this.isOverdraw = isOverdraw;
        }

        @Override public int compareTo(Payload o) {
            if (this.priority == o.priority) {
                return Long.compare(this.seq, o.seq);
            }
            return this.priority - o.priority;
        }

        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            Payload payload = (Payload) o;

            if (id != payload.id)
                return false;
            if (seq != payload.seq)
                return false;
            if (priority != payload.priority)
                return false;
            if (isOverdraw != payload.isOverdraw)
                return false;
            if (type != payload.type)
                return false;
            return sqlCtx != null ? sqlCtx.equals(payload.sqlCtx) : payload.sqlCtx == null;

        }

        @Override public int hashCode() {
            int result = id;
            result = 31 * result + (type != null ? type.hashCode() : 0);
            result = 31 * result + (sqlCtx != null ? sqlCtx.hashCode() : 0);
            result = 31 * result + (int) (seq ^ (seq >>> 32));
            result = 31 * result + priority;
            result = 31 * result + (isOverdraw ? 1 : 0);
            return result;
        }

        @Override public boolean isOverdraw() {
            return this.isOverdraw;
        }
    }


    public static enum PayloadType {
        QUERY, TRX
    }

    public int availablePermits(int selectorId) {
        return this.mulSemaphore.availablePermits(selectorId);
    }

    public synchronized List<String> getInfo() {
        final List<String> info = new ArrayList<>();
        this.registeredIds.stream().forEach(id -> {
            StringBuilder sb = new StringBuilder();
            sb.append(this.mulQueue.queueName(id) + " ");
            sb.append("id" + "=" + id + " ");
            sb.append("queue" + "=" + this.mulQueue.size(id) + " ");
            sb.append("Semaphore" + "=" + this.mulSemaphore.availablePermits(id));
            info.add(sb.toString());
        });
        return info;
    }

    public BitSet getRegisteredIds() {
        return registeredIds;
    }

    public Scheduler getScheduler(int id) {
        return registeredScheduler[id];
    }

    class Worker extends Thread {
        @Override public void run() {
            long[] modCounts = new long[capacity];
            while (true) {
                DALPermitsSet permitsSet = null;
                DALPermitsSet retryPermitsSet = null;
                DALPermitsSet overdrawPermitsSet = null;
                List<Payload> payloads = null;
                try {
                    //信号量快照，包括有信号量有消息和无信号量有消息的阻塞队列id及其信号量许可
                    permitsSet = mulSemaphore.tryAcquire(mulQueue.getReadyQueueIds());
                    //获取所有有信号量有消息的Schduler的消息
                    payloads = mulQueue.take(permitsSet.getIds());

                    //遍历bitSet, LOG记录阻塞的队列
                    for (int id = permitsSet.getMisIds().nextSetBit(0);
                         id >= 0; id = permitsSet.getMisIds().nextSetBit(id + 1)) {
                        Scheduler shde = registeredScheduler[id];
                        long modCount = mulQueue.getModCount(id);
                        if (modCounts[id] != modCount && shde != null) {
                            MetricFactory.newCounterWithScheduler(Metrics.SEMAPHORE_EMPTY, shde)
                                .once();
                            LOGGER.warn(String
                                .format("[%s] Semaphore availablePermits == 0 queue len is %s ",
                                    mulQueue.queueName(id), mulQueue.size(id)));
                        }
                        modCounts[id] = modCount;
                    }
                    if (LOGGER.isDebugEnabled()) {
                        for (int id = permitsSet.getIds().nextSetBit(0);
                             id >= 0; id = permitsSet.getIds().nextSetBit(id + 1)) {
                            Scheduler shde = registeredScheduler[id];
                            if (shde != null) {
                                LOGGER.debug(String
                                    .format("[%s] Semaphore availablePermits == [%s]",
                                        mulQueue.queueName(id), mulSemaphore.availablePermits(id)));
                            }
                        }
                    }

                    //处理事务透支信号量的逻辑
                    if (!permitsSet.getMisIds().isEmpty()) {
                        //拿出有消息无信号量的阻塞队列Id, 无论该队列是否是事务队列
                        overdrawPermitsSet = mulSemaphore.overdrawAcquire(permitsSet.getMisIds());
                        //从有消息的阻塞队列中取出可透支的消息
                        payloads.addAll(mulQueue.pollOverDraw(overdrawPermitsSet.getIds()));
                        permitsSet.merge(overdrawPermitsSet);
                        overdrawPermitsSet.release();
                        overdrawPermitsSet = null;
                    }

                    //等待1毫秒后(让渡CPU时间片),继续尝试获取信号量，目前处于有SqlCtx没有信号量的状态
                    if (payloads.isEmpty() && !permitsSet.getMisIds().isEmpty()) {
                        retryPermitsSet = mulSemaphore
                            .tryAcquire(1, TimeUnit.MILLISECONDS, permitsSet.getMisIds());
                        permitsSet.merge(retryPermitsSet);
                        retryPermitsSet.release();
                        retryPermitsSet = null;
                        payloads.addAll(mulQueue.take(permitsSet.getIds()));
                    }

                    for (Payload payload : payloads) {
                        Scheduler sche = registeredScheduler[payload.id];
                        if (sche == null) {
                            payload.sqlCtx.kill(ErrorCode.ABORT, "Scheduler Shutdown");
                            continue;
                        }
                        DALPermits permits = permitsSet.take(payload.id);
                        if (payload.type == PayloadType.TRX) {
                            //限制开启的最大事务数,SqlCtx占用事务信号量,但是不继续向下执行,继续放入限流队列中
                            payload.sqlCtx.setTrxSemaphore(permits);
                            sche.enqueue(payload.sqlCtx, PayloadType.QUERY, true);
                        } else {
                            if (sche.getInfo().getRole() == DBRole.GRAY) {
                                //不再会运行到的GRAY逻辑，伴随GRAY一起废弃
                                payload.sqlCtx.grayUp.setDbSemaphore(permits);
                            } else {
                                //为SqlCtx设置信号量许可，以便SqlCtx写回所有响应到client后释放信号量
                                payload.sqlCtx.setDbSemaphore(permits);
                            }
                            //经过限流,带着信号量,SqlCtx继续向下执行
                            SchedulerWorker.getInstance()
                                .enqueue("MulSelector run", payload.sqlCtx);
                        }
                    }
                    permitsSet.release();
                    permitsSet = null;
                } catch (Throwable t) {
                    LOGGER.error(Objects.toString(t));
                    try {
                        if (permitsSet != null) {
                            permitsSet.release();
                        }
                        if (retryPermitsSet != null) {
                            retryPermitsSet.release();
                        }
                        if (overdrawPermitsSet != null) {
                            overdrawPermitsSet.release();
                        }
                        if (payloads != null) {
                            for (Payload payload : payloads) {
                                payload.sqlCtx.kill(ErrorCode.ABORT, "MulScheduler ERROR");
                            }
                        }
                    } catch (Exception throwable) {
                        LOGGER.error("MulScheduler ERROR", throwable);
                        // throwable.printStackTrace();
                    }
                }
            }
        }
    }
}
