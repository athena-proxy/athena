package me.ele.jarch.athena.netty;

import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import me.ele.jarch.athena.constant.Metrics;
import me.ele.jarch.athena.util.etrace.MetricFactory;

/**
 * Created by jinghao.wang on 17/5/26.
 */
public class EventLoopGroupMonitorJob implements Monitorable {
    private final EventLoopGroup eventLoopGroup;
    private final String eventLoopGroupType;

    public EventLoopGroupMonitorJob(EventLoopGroup eventLoopGroup, String eventLoopGroupType) {
        this.eventLoopGroup = eventLoopGroup;
        this.eventLoopGroupType = eventLoopGroupType;
    }

    @Override public void monitor() {
        eventLoopGroup.forEach(eventExecutor -> {
            if (eventExecutor instanceof SingleThreadEventExecutor) {
                int tasks = ((SingleThreadEventExecutor) eventExecutor).pendingTasks();
                MetricFactory.newTimer(Metrics.NETTY_QUEUE_SIZE)
                    .addTag("EventLoopGroupType", eventLoopGroupType).setUpperEnable(false)
                    .value(tasks);
            }
        });
    }
}
