package me.ele.jarch.athena.util;

import me.ele.jarch.athena.util.MulSemaphore.DALPermitsSet;
import me.ele.jarch.athena.util.schedule.DalQueueEntry;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.BitSet;
import java.util.List;

public class MulBlockingQueueSemaphoreTest {

    @Test public void takeWithSemaphore() throws InterruptedException {
        final MulBlockingQueue<Payload> mq = new MulBlockingQueue<>(1024);
        BitSet indexs = new BitSet(1024);
        indexs.set(mq.register("eosgroup1"));
        indexs.set(mq.register("eosgroup2"));
        indexs.set(mq.register("eosgroup3"));
        mq.put(new Payload(0, "package"), 0);
        mq.put(new Payload(1, "package"), 1);
        mq.put(new Payload(2, "package"), 2);
        final MulSemaphore ms = new MulSemaphore(1024);
        indexs.stream().forEach(i -> {
            ms.putPermits(i, 0, 0);
        });
        ms.putPermits(0, 1, 0);
        DALPermitsSet permitsSet = ms.acquire(mq.getReadyQueueIds());
        List<Payload> payloads = mq.take(permitsSet.getIds());
        Assert.assertEquals(payloads.get(0).id, 0);
        Assert.assertTrue(ms.tryAcquire(indexs).getIds().isEmpty());

        ms.putPermits(0, 1, 0);
        permitsSet = ms.tryAcquire(indexs);
        payloads = mq.take(permitsSet.getIds());
        Assert.assertTrue(payloads.isEmpty());
        indexs.andNot(permitsSet.getIds());
        Assert.assertEquals(indexs.stream().toArray(), new int[] {1, 2});
        Assert.assertTrue(ms.tryAcquire(indexs).getIds().isEmpty());

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

}
