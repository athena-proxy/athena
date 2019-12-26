package me.ele.jarch.athena.scheduler;

import me.ele.jarch.athena.util.JobScheduler;

import java.util.concurrent.atomic.AtomicLong;

public class RequestThrottle {
    private final AtomicLong executedCount = new AtomicLong(0);
    private final static JobScheduler REQUEST_THROTTLE_JOB_SCHEDULER =
        new JobScheduler("REQUEST_THROTTLE_JOB_SCHEDULER ");

    static {
        REQUEST_THROTTLE_JOB_SCHEDULER.start();
    }

    public long getExecutedCount() {
        return executedCount.get();
    }


    public void enqueuePayloadWithDelayInMillis(String payload, long delayInMillis) {
        REQUEST_THROTTLE_JOB_SCHEDULER
            .addOneTimeJob("string payload", delayInMillis, () -> this.execute(payload));
    }

    public void enqueuePayloadWithDelayInMillis(Runnable runnable, long delayInMillis) {
        REQUEST_THROTTLE_JOB_SCHEDULER
            .addOneTimeJob("string payload", delayInMillis, () -> this.execute(runnable));
    }

    private void execute(String payload) {
        this.executedCount.incrementAndGet();
    }

    private void execute(Runnable runnable) {
        runnable.run();
        this.executedCount.incrementAndGet();
    }
}
