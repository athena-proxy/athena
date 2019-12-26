package me.ele.jarch.athena.scheduler;

import java.util.concurrent.atomic.AtomicLong;

public class SQLTrafficPolicing {
    final RequestThrottle requestThrottle = new RequestThrottle();
    final String sqlid;
    final AtomicLong lastTime = new AtomicLong();
    final double rate;
    final long period;

    public SQLTrafficPolicing(String sqlid, double rate) {
        this.sqlid = sqlid;
        this.rate = rate;
        this.period = (long) (1000.0 / rate);
    }

    public void doPolicing(Runnable runnable) {
        final long now = System.currentTimeMillis();
        long sendTime = lastTime.updateAndGet(n -> n + period > now ? n + period : now);
        long delay = sendTime - now;
        requestThrottle.enqueuePayloadWithDelayInMillis(runnable, delay);
    }

    @Override public String toString() {
        return "SQLTrafficPolicing [requestThrottle=" + requestThrottle + ", sqlid=" + sqlid
            + ", lastTime=" + lastTime + ", rate=" + rate + ", period=" + period + "]";
    }

}
