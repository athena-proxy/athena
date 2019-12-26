package me.ele.jarch.athena.util;

import me.ele.jarch.athena.util.schedule.DalQueueEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class MulBlockingQueue<E extends DalQueueEntry> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MulBlockingQueue.class);
    private final int maxQueueNumber;
    /**
     * Lock held by take
     */
    private final ReentrantLock takeLock = new ReentrantLock();
    /**
     * Wait queue for waiting takes
     */
    private final Condition notEmpty = takeLock.newCondition();
    private final BitSet readyQueueIds;

    private final DalQueue[] queues;

    public MulBlockingQueue(int maxQueueNumber) {
        this.maxQueueNumber = maxQueueNumber;
        this.queues = new DalQueue[maxQueueNumber];
        this.readyQueueIds = new BitSet(maxQueueNumber);
    }

    public int size(int queueId) {
        DalQueue dalqueue = this.queues[queueId];
        return dalqueue == null ? 0 : dalqueue.count.get();
    }

    public String queueName(int queueId) {
        DalQueue dalqueue = this.queues[queueId];
        return dalqueue == null ? "" : dalqueue.name;
    }

    public void put(E e, int queueId) throws InterruptedException {
        int c = -1;
        if (e == null) {
            return;
        }
        DalQueue dalqueue = queues[queueId];
        if (dalqueue == null) {
            LOGGER.error("cannot found the queue of id:{}", queueId);
            return;
        }
        dalqueue.queue.add(e);

        c = dalqueue.count.getAndIncrement();
        dalqueue.modCount.getAndIncrement();
        if (c == 0) {
            signalNotEmpty(queueId);
        }
    }

    /**
     * poll object from all the specified queues.
     *
     * @param queueIds
     * @return the objects poll from the specified queues
     * @throws InterruptedException
     */
    @SuppressWarnings("unchecked") public List<E> poll(BitSet queueIds)
        throws InterruptedException {
        ArrayList<E> re = new ArrayList<>(queueIds.cardinality());
        for (int id = queueIds.nextSetBit(0); id >= 0; id = queueIds.nextSetBit(id + 1)) {
            DalQueue dalqueue = queues[id];
            if (dalqueue == null) {
                unReadyQueue(id, dalqueue);
                continue;
            }
            E x = (E) dalqueue.queue.poll();
            if (x != null) {
                dalqueue.count.getAndDecrement();
                dalqueue.modCount.getAndIncrement();
                re.add(x);
            }
            if (dalqueue.count.get() == 0) {
                unReadyQueue(id, dalqueue);
            }

        }
        return re;
    }

    /**
     * poll the overdraw object from all the specified queues.
     *
     * @param queueIds
     * @return the objects poll from the specified queues
     * @throws InterruptedException
     */
    @SuppressWarnings("unchecked") public List<E> pollOverDraw(BitSet queueIds)
        throws InterruptedException {
        ArrayList<E> re = new ArrayList<>(queueIds.cardinality());
        for (int id = queueIds.nextSetBit(0); id >= 0; id = queueIds.nextSetBit(id + 1)) {
            DalQueue dalqueue = queues[id];
            if (dalqueue == null) {
                unReadyQueue(id, dalqueue);
                continue;
            }
            E entry = (E) dalqueue.queue.peek();
            if (entry == null || !entry.isOverdraw()) {
                continue;
            }
            E x = (E) dalqueue.queue.poll();
            if (x != null) {
                if (entry == x) {
                    dalqueue.count.getAndDecrement();
                    dalqueue.modCount.getAndIncrement();
                    re.add(x);
                } else {
                    dalqueue.queue.add(x);
                }
            }
            if (dalqueue.count.get() == 0) {
                unReadyQueue(id, dalqueue);
            }
        }
        return re;
    }

    private void unReadyQueue(int id, DalQueue dalqueue) throws InterruptedException {
        final ReentrantLock takeLock = this.takeLock;
        takeLock.lockInterruptibly();
        try {
            if (dalqueue == null || dalqueue.count.get() == 0) {
                this.readyQueueIds.clear(id);
            }
        } finally {
            takeLock.unlock();
        }
    }

    public List<E> poll(long timeout, TimeUnit unit, BitSet queueIds) throws InterruptedException {
        final ReentrantLock takeLock = this.takeLock;
        long nanos = unit.toNanos(timeout);
        takeLock.lockInterruptibly();
        try {
            while (this.readyQueueIds.isEmpty()) {
                if (nanos < 0) {
                    return new ArrayList<>();
                }
                nanos = notEmpty.awaitNanos(nanos);
            }
        } finally {
            takeLock.unlock();
        }
        List<E> re = poll(queueIds);
        if (!this.readyQueueIds.isEmpty()) {
            signalNotEmpty();
        }
        return re;
    }

    public List<E> take(BitSet queueIds) throws InterruptedException {
        List<E> re = poll(queueIds);
        if (!re.isEmpty()) {
            return re;
        }
        final ReentrantLock takeLock = this.takeLock;
        takeLock.lockInterruptibly();
        try {
            while (this.readyQueueIds.isEmpty()) {
                notEmpty.await();
            }
        } finally {
            takeLock.unlock();
        }
        re = poll(queueIds);
        if (!this.readyQueueIds.isEmpty()) {
            signalNotEmpty();
        }
        return re;
    }

    /**
     * Signals a waiting take. Called only from put (which do not otherwise ordinarily lock takeLock.)
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

    private void signalNotEmpty(int queueId) {
        final ReentrantLock takeLock = this.takeLock;
        takeLock.lock();
        try {
            readyQueueIds.set(queueId);
            notEmpty.signal();
        } finally {
            takeLock.unlock();
        }
    }

    synchronized public int register(String queueName) {
        if (queueName == null || queueName.isEmpty()) {
            LOGGER.error("invalid queueName");
            throw new RuntimeException("invalid queueName");
        }
        int idleId = -1;
        for (int id = 0; id < this.maxQueueNumber; id++) {
            if (queues[id] == null) {
                idleId = id;
                break;
            }
        }
        if (idleId == -1) {
            LOGGER.error("out of Mulscheduler Capacity");
            throw new RuntimeException("out of Mulscheduler Capacity");
        }
        queues[idleId] =
            new DalQueue(new PriorityBlockingQueue<Object>(), new AtomicInteger(), queueName);
        return idleId;
    }

    synchronized public void unregister(int id) {
        queues[id] = null;
    }

    /**
     * @return 当前已使用的queue槽位数。
     */
    public int queueSize() {
        int queueSize = 0;
        for (int id = 0; id < queues.length; id++) {
            if (Objects.nonNull(queues[id])) {
                queueSize++;
            }
        }
        return queueSize;
    }

    @Override public String toString() {
        final StringBuilder sb = new StringBuilder();
        for (int id = 0; id < this.maxQueueNumber; id++) {
            DalQueue q = this.queues[id];
            if (q == null) {
                continue;
            }
            sb.append(q.name + " ");
            sb.append("id" + "=" + id + ",");
            int size = q.count.get();
            sb.append("size=" + size + ";");
        }
        return "MulBlockingQueue [" + sb.toString() + "]";
    }

    static class DalQueue {
        public final Queue<Object> queue;
        public final AtomicInteger count;
        public final String name;
        public final AtomicLong modCount = new AtomicLong();

        public DalQueue(Queue<Object> queue, AtomicInteger count, String name) {
            super();
            this.queue = queue;
            this.count = count;
            this.name = name;
        }
    }

    public BitSet getReadyQueueIds() {
        return readyQueueIds;
    }

    public long getModCount(int queueId) {
        DalQueue dalqueue = queues[queueId];
        if (dalqueue == null) {
            return 0;
        }
        return dalqueue.modCount.get();
    }
}
