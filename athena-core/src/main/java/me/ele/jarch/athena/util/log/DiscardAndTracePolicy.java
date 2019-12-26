package me.ele.jarch.athena.util.log;

import me.ele.jarch.athena.constant.Metrics;
import me.ele.jarch.athena.util.etrace.MetricFactory;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author jinghao.wang
 */
public class DiscardAndTracePolicy extends ThreadPoolExecutor.DiscardPolicy {
    @Override public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
        super.rejectedExecution(r, e);
        MetricFactory.newCounter(Metrics.DANGER_SQL_FILTER_DISCARD).once();
    }
}
