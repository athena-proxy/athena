package me.ele.jarch.athena.scheduler;

import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.constant.Metrics;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.netty.state.SESSION_STATUS;
import me.ele.jarch.athena.util.GreySwitch;
import me.ele.jarch.athena.util.JobMethod;
import me.ele.jarch.athena.util.JobScheduler;
import me.ele.jarch.athena.worker.SchedulerWorker;
import me.ele.jarch.athena.worker.manager.ClientLoginTimeoutManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Monitor DAL client login slow/timeout
 */

public class ClientLoginMonitor {
    private static final Logger logger = LoggerFactory.getLogger(ClientLoginMonitor.class);

    private static final ClientLoginMonitor INSTANCE = new ClientLoginMonitor();
    private static final int MAX_CAPACITY = 20_000;
    private static final AtomicInteger currentJobCounts = new AtomicInteger(0);

    private static final String TIMEOUT_ERROR =
        "Got a login connection timeout, session info:[{}], client info:[{}], login timeline info: [{}]";
    private static final String SLOW_ERROR =
        "Got a login connection slow, session info:[{}], client info:[{}], login timeline info: [{}]";
    private static final String SUSPECT_PING_ERROR =
        "Got a login client suspectual ping, session info:[{}], client info:[{}], login timeline info: [{}]";


    private static final JobScheduler scheduler = new JobScheduler("Client-Login-Monitor-Job");

    static {
        scheduler.start();
    }

    private ClientLoginMonitor() {
    }

    public static ClientLoginMonitor getInstance() {
        return INSTANCE;
    }

    /**
     * Raise a warning to Metrics if the client can't finish login within certain time
     */
    public void addSlowWarningJob(SqlSessionContext ctx) {
        if (!loginSlowEnabled()) {
            return;
        }
        if (currentJobCounts.get() < MAX_CAPACITY) {
            currentJobCounts.getAndIncrement();
            scheduler.addOneTimeJob("clientLoginSlowMonitor", 10, new SlowMonitorJob(ctx));
        } else {
            logger.error(
                "The Client-Login-Monitor-Job queue reached max capacity.the client address is {}",
                ctx.getClientAddr());
        }
    }


    private boolean loginTimeoutEnabled() {
        return GreySwitch.getInstance().getLoginTimeoutInMills() > 0;
    }

    private boolean loginSlowEnabled() {
        return GreySwitch.getInstance().getLoginSlowInMills() > 0;
    }

    private long getLoginTimeoutInMills() {
        return GreySwitch.getInstance().getLoginTimeoutInMills();
    }

    private long getSlowLoginInMills() {
        return GreySwitch.getInstance().getLoginSlowInMills();
    }

    private class SlowMonitorJob implements JobMethod {
        private final long expectSlowTime;
        private final long expectTimeoutTime;

        private SqlSessionContext ctx;

        private boolean isSlowReached = false;

        SlowMonitorJob(SqlSessionContext ctx) {
            this.ctx = ctx;
            expectSlowTime = System.currentTimeMillis() + getSlowLoginInMills();
            expectTimeoutTime = System.currentTimeMillis() + getLoginTimeoutInMills();
        }

        @Override public void invoke() throws Exception {
            if (authDone()) {
                currentJobCounts.getAndDecrement();
                return;
            }
            if (isCtxInQuitStat()) {
                currentJobCounts.getAndDecrement();
                if (isSuspectPing()) {
                    ctx.sqlSessionContextUtil
                        .sendSuspectPingMetric(Metrics.LOGIN_SUSPECT_PING, SUSPECT_PING_ERROR);
                }
                return;
            }
            if (System.currentTimeMillis() > expectSlowTime && !isSlowReached) {
                ctx.sqlSessionContextUtil.sendLoginErrMetric(Metrics.LOGIN_SLOW, SLOW_ERROR);
                isSlowReached = true;
                if (!loginTimeoutEnabled()) {
                    currentJobCounts.getAndDecrement();
                    return;
                }
            }

            if (System.currentTimeMillis() > expectTimeoutTime) {
                SchedulerWorker.getInstance().enqueue(new ClientLoginTimeoutManager(ctx, () -> {
                    try {
                        ctx.sqlSessionContextUtil.appendLoginPhrase(Constants.AUTH_TIMEOUT)
                            .appendLoginTimeLineEtrace().endEtrace();
                        ctx.doQuit();
                        ctx.sqlSessionContextUtil
                            .sendLoginErrMetric(Metrics.LOGIN_TIMEOUT, TIMEOUT_ERROR);
                    } finally {
                        currentJobCounts.getAndDecrement();
                    }
                }));
                return;
            }

            scheduler.addOneTimeJob("clientLoginMonitor", 10, this);
        }

        private boolean authDone() {
            return ctx.authenticator.isAuthenticated || ctx.authenticator.isDalHealthCheckUser;
        }

        private boolean isSuspectPing() {
            return ctx.sqlSessionContextUtil.cRecentRecvTime == 0;
        }

        private boolean isCtxInQuitStat() {
            return ctx.getStatus() == SESSION_STATUS.QUIT;
        }
    }

}
