package me.ele.jarch.athena.scheduler;

import me.ele.jarch.athena.constant.Metrics;
import me.ele.jarch.athena.netty.Monitorable;
import me.ele.jarch.athena.util.ZKCache;
import me.ele.jarch.athena.util.etrace.MetricFactory;

public class SchedulerMonitorJob implements Monitorable {
    private static final int WATCH_WINDOWS = 5;

    public SchedulerMonitorJob(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    private Scheduler scheduler;
    private WindowCounter queueSizeCounter = new WindowCounter(WATCH_WINDOWS);

    @Override public void monitor() {
        int queueSize = scheduler.getQueueBlockingTaskCount();
        ZKCache zkCache = scheduler.getZKCache();
        MetricFactory.newTimerWithScheduler(Metrics.SCHEDULER_QUEUE_SIZE, scheduler)
            .setUpperEnable(false).value(queueSize);

        queueSizeCounter.record(queueSize);
        final int upper = zkCache.getSmartUpperAutoKillerSize();
        if (queueSizeCounter.allMatch(i -> i > upper) && zkCache.isAutoKillerOpen()) {
            AutoKiller.startAutoKiller(scheduler);
        }
    }
}
