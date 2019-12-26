package me.ele.jarch.athena.worker;

import me.ele.jarch.athena.constant.Metrics;
import me.ele.jarch.athena.netty.Monitorable;
import me.ele.jarch.athena.util.etrace.MetricFactory;

import java.util.concurrent.BlockingQueue;

/**
 * Created by jinghao.wang on 16/6/13.
 */
public class SchedulerWorkerMonitorJob implements Monitorable {

    private final BlockingQueue<Runnable> queue;

    public SchedulerWorkerMonitorJob(BlockingQueue<Runnable> queue) {
        this.queue = queue;
    }

    @Override public void monitor() {
        int wokerQueueSize = queue.size();
        MetricFactory.newTimer(Metrics.WORKER_QUEUE_SIZE).setUpperEnable(false)
            .value(wokerQueueSize);
    }
}
