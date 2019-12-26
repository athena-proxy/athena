package me.ele.jarch.athena.util.etrace;

import io.etrace.agent.Trace;
import io.etrace.common.modal.metric.Counter;
import io.etrace.common.modal.metric.Gauge;
import io.etrace.common.modal.metric.Timer;
import io.etrace.common.modal.metric.impl.CounterEmpty;
import io.etrace.common.modal.metric.impl.GaugeEmpty;
import io.etrace.common.modal.metric.impl.TimerEmpty;
import me.ele.jarch.athena.allinone.DBConnectionInfo;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.constant.TraceNames;
import me.ele.jarch.athena.exception.AuthQuitState;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.scheduler.Scheduler;
import me.ele.jarch.athena.server.pool.ServerSession;

import java.util.Objects;

/**
 * 对Etrace Metric Client的包装类,为了提供灰度上线,以及在极端情况下
 * DAL单节点降级Etrace Metric功能
 * Created by jinghao.wang on 2017/9/7.
 */
public class MetricFactory {
    private static final Counter COUNTER_EMPTY = new CounterEmpty();
    private static final Gauge GAUGE_EMPTY = new GaugeEmpty();
    private static final Timer TIMER_EMPTY = new TimerEmpty();

    private static volatile boolean enable = true;

    private MetricFactory() {
    }

    public static void setEnable(boolean value) {
        enable = value;
    }

    /**
     * Counter with hostname tag
     *
     * @param name
     * @return
     */
    public static Counter newCounter(String name) {
        return enable ?
            Trace.newCounter(name).addTag(TraceNames.HOSTNAME, Constants.HOSTNAME) :
            COUNTER_EMPTY;
    }

    /**
     * Gauge with hostname tag
     *
     * @param name
     * @return
     */
    public static Gauge newGauge(String name) {
        return enable ?
            Trace.newGauge(name).addTag(TraceNames.HOSTNAME, Constants.HOSTNAME) :
            GAUGE_EMPTY;
    }

    /**
     * Timer with hostname tag
     *
     * @param name
     * @return
     */
    public static Timer newTimer(String name) {
        return enable ?
            Trace.newTimer(name).addTag(TraceNames.HOSTNAME, Constants.HOSTNAME) :
            TIMER_EMPTY;
    }

    public static Counter newCounterWithSqlSessionContext(String name, SqlSessionContext ctx) {
        return newCounter(name)
            .addTag(TraceNames.DALGROUP, MetricMetaExtractor.extractDALGroupName(ctx))
            .addTag(TraceNames.DBID, MetricMetaExtractor.extractDBid(ctx))
            .addTag(TraceNames.SQL_TYPE, ctx.getQueryType().name());
    }

    public static Counter newQuitCounter(String name, SqlSessionContext ctx,
        AuthQuitState quitState) {
        return newCounter(name).addTag(TraceNames.DALGROUP, Objects.isNull(ctx.getHolder()) ?
            ctx.authenticator.getSchema() :
            ctx.getHolder().getCfg()).addTag(TraceNames.QUIT_REASON, quitState.name());
    }

    public static Counter newCounterWithScheduler(String name, Scheduler scheduler) {
        return newCounter(name)
            .addTag(TraceNames.DALGROUP, MetricMetaExtractor.extractDALGroupName(scheduler))
            .addTag(TraceNames.DBID, MetricMetaExtractor.extractDBid(scheduler))
            .addTag(TraceNames.DATABASE, MetricMetaExtractor.extractDatabase(scheduler))
            .addTag(TraceNames.DB_IP, MetricMetaExtractor.extractDBIp(scheduler))
            .addTag(TraceNames.DB_PORT, MetricMetaExtractor.extractDBPort(scheduler));
    }

    public static Counter newCounterWithDBConnectionInfo(String name, DBConnectionInfo info) {
        return newCounter(name)
            .addTag(TraceNames.DBGROUP, MetricMetaExtractor.extractDBGroupName(info))
            .addTag(TraceNames.DBID, MetricMetaExtractor.extractDBid(info))
            .addTag(TraceNames.DATABASE, MetricMetaExtractor.extractDatabase(info))
            .addTag(TraceNames.DB_IP, MetricMetaExtractor.extractDBIp(info))
            .addTag(TraceNames.DB_PORT, MetricMetaExtractor.extractDBPort(info));
    }

    public static Counter newCounterWithServerSession(String name, ServerSession serverSession) {
        return newCounter(name)
            .addTag(TraceNames.DALGROUP, MetricMetaExtractor.extractDALGroupName(serverSession))
            .addTag(TraceNames.DBID, MetricMetaExtractor.extractDBid(serverSession));
    }

    public static Timer newTimerWithSqlSessionContext(String name, SqlSessionContext ctx) {
        return newTimer(name)
            .addTag(TraceNames.DALGROUP, MetricMetaExtractor.extractDALGroupName(ctx))
            .addTag(TraceNames.DBID, MetricMetaExtractor.extractDBid(ctx))
            .addTag(TraceNames.SQL_TYPE, ctx.getCurQueryType().name());
    }

    public static Timer newTimerWithScheduler(String name, Scheduler scheduler) {
        return newTimer(name)
            .addTag(TraceNames.DALGROUP, MetricMetaExtractor.extractDALGroupName(scheduler))
            .addTag(TraceNames.DBID, MetricMetaExtractor.extractDBid(scheduler))
            .addTag(TraceNames.DATABASE, MetricMetaExtractor.extractDatabase(scheduler));
    }

    public static Gauge newGaugeWithScheduler(String name, Scheduler scheduler) {
        return newGauge(name)
            .addTag(TraceNames.DALGROUP, MetricMetaExtractor.extractDALGroupName(scheduler))
            .addTag(TraceNames.DBID, MetricMetaExtractor.extractDBid(scheduler))
            .addTag(TraceNames.DATABASE, MetricMetaExtractor.extractDatabase(scheduler))
            .addTag(TraceNames.DB_IP, MetricMetaExtractor.extractDBIp(scheduler))
            .addTag(TraceNames.DB_PORT, MetricMetaExtractor.extractDBPort(scheduler));
    }

    public static Gauge newGaugeWithSqlSessionContext(String name, SqlSessionContext ctx) {
        return newGauge(name)
            .addTag(TraceNames.DALGROUP, MetricMetaExtractor.extractDALGroupName(ctx))
            .addTag(TraceNames.DBID, MetricMetaExtractor.extractDBid(ctx))
            .addTag(TraceNames.SQL_TYPE, ctx.getQueryType().name());
    }
}
