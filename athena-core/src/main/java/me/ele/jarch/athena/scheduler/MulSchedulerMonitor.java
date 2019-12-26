package me.ele.jarch.athena.scheduler;

import me.ele.jarch.athena.constant.Metrics;
import me.ele.jarch.athena.netty.Monitorable;
import me.ele.jarch.athena.util.etrace.MetricFactory;

/**
 * Created by jinghao.wang on 17/6/13.
 */
public class MulSchedulerMonitor implements Monitorable {
    private final MulScheduler mulScheduler;

    public MulSchedulerMonitor(MulScheduler mulScheduler) {
        this.mulScheduler = mulScheduler;
    }

    @Override public void monitor() {
        int queues = mulScheduler.size();
        MetricFactory.newTimer(Metrics.MULSCHEDULER_QUEUE_SIZE).setUpperEnable(false).value(queues);
    }
}
