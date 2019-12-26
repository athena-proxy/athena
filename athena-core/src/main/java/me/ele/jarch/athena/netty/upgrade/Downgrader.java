package me.ele.jarch.athena.netty.upgrade;

import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.constant.Metrics;
import me.ele.jarch.athena.constant.TraceNames;
import me.ele.jarch.athena.netty.AthenaFrontServer;
import me.ele.jarch.athena.netty.AthenaServer;
import me.ele.jarch.athena.netty.AthenaUtils;
import me.ele.jarch.athena.scheduler.HangSessionMonitor;
import me.ele.jarch.athena.util.etrace.MetricFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author shaoyang.qi
 * <p>
 * 机器的降级下线控制逻辑
 */
public class Downgrader {
    private static final Logger logger = LoggerFactory.getLogger(Downgrader.class);
    private long startDyingTime = 0;

    public void triggerDowngrade() {
        logger.error(AthenaUtils.pidStr() + " is downgrading");
        MetricFactory.newCounter(Metrics.DAL_UPGRADE).addTag("Role", Constants.ROLE)
            .addTag(TraceNames.STATUS, String.valueOf(UpgradeCenter.STATUS.DOWNGRADE_TRIGGERED))
            .once();
        closeListener();

        AthenaServer.commonJobScheduler.addJob("waiting_for_client_connection_finish", 5000, () -> {
            boolean isEmpty = HangSessionMonitor.getAllCtx().isEmpty();
            if (isEmpty || isTimeout(System.currentTimeMillis())) {
                die();
            }
        });
    }

    private void closeListener() {
        logger.info(AthenaUtils.pidStr() + " is closeListener .... ");
        AthenaFrontServer.getInstance().closeBossChannel(Constants.SMOOTH_DOWN_DELAY_IN_MILLS);
        startDyingTime = System.currentTimeMillis();
    }

    private boolean isTimeout(long timeMillis) {
        return (timeMillis - startDyingTime > Constants.DYING_TIME_LIMIT);
    }

    private void die() throws Exception {
        logger.info(AthenaUtils.pidStr() + " dies!");
        Thread.sleep(2000);
        System.exit(0); // NOSONAR
    }
}
