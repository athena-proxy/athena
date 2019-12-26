package me.ele.jarch.athena.netty.upgrade;

import io.netty.channel.epoll.Epoll;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.constant.Metrics;
import me.ele.jarch.athena.constant.TraceNames;
import me.ele.jarch.athena.netty.AthenaServer;
import me.ele.jarch.athena.netty.AthenaUtils;
import me.ele.jarch.athena.util.NoThrow;
import me.ele.jarch.athena.util.etrace.MetricFactory;
import org.apache.commons.io.comparator.LastModifiedFileComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author shaoyang.qi
 * <p>
 * 不间断升级中心，为athena提供不间断升级功能。
 * <p>
 * 包括普通的hotSpot机器的不间断升级和zing机器的不间断升级。
 * <p>
 * hotSpot不间断升级使用默认的升级策略，即两个hotSpot机器之间直接切换升级。
 * <p>
 * zing机器升级需要使用中间人策略，即通过hotSpot作为一个中间人，控制两个zing机器的升级切换。
 * <p>
 * 两种策略均使用shell实现
 */
@SuppressWarnings("restriction") public class UpgradeCenter {
    private static final Logger logger = LoggerFactory.getLogger(UpgradeCenter.class);
    private final UdsServer upgradeServer = new UdsServer(Constants.UPGRADE_AUDS_PATH);
    private final UdsServer downgradeServer = new UdsServer(Constants.DOWNGRADE_AUDS_PATH);
    private final AtomicBoolean upgradeFinished = new AtomicBoolean(false);
    private final AtomicBoolean downgradeTriggered = new AtomicBoolean(false);
    private Downgrader downgrader = null;

    private static final AtomicBoolean isUSR2SignalReceived = new AtomicBoolean(false);


    public enum STATUS {SIGNAL_RECEIVED, CLOSE_UPGRADE_UDS, CLOSE_LISTEN_PORTS, MEDIATOR_KILLER_TRIGGERED, DOWNGRADE_TRIGGERED, FAILED}

    private UpgradeCenter() {
    }

    private static class INNER {
        private static final UpgradeCenter INSTANCE = new UpgradeCenter();
    }

    public static UpgradeCenter getInstance() {
        return INNER.INSTANCE;
    }

    public void finishUpgrade() {
        if (!upgradeFinished.compareAndSet(false, true)) {
            return;
        }
        AthenaServer.quickJobScheduler.addOneTimeJob("close_upgrade_adus_server", 1, () -> {
            NoThrow.execute(() -> upgradeServer.stop(), (e) -> {
                logger.error(AthenaUtils.pidStr() + " stop uds server error", e);
                upgradeFinished.compareAndSet(true, false);
            });
        });
    }

    public void startDowngrade() {
        if (downgrader == null) {
            return;
        }
        if (!downgradeTriggered.compareAndSet(false, true)) {
            logger.info(AthenaUtils.pidStr() + " downgrade has been triggered");
            return;
        }
        logger.error(AthenaUtils.pidStr() + " start to downgrade");
        downgrader.triggerDowngrade();
    }

    public void startUpgrade() {
        if (!Epoll.isAvailable()) {
            logger.error(AthenaUtils.pidStr()
                + " epoll is not available,can't start upgrade UdsServer,exit!");
            exit();
        }
        NoThrow.execute(() -> upgradeServer.start(), (e) -> {
            logger.error(AthenaUtils.pidStr() + " start upgrade UdsServer error,exit", e);
            exit();
        });
        // add job to close upgradeServer automatically in case not received command from shell
        AthenaServer.quickJobScheduler
            .addOneTimeJob("UpgradeCenter_finish_upgrade", Constants.UPGRADE_TIME_LIMIT,
                () -> this.finishUpgrade());
    }

    public void registerSignalHandler() {
        downgrader = new Downgrader();

        if (Constants.ROLE.equals(Constants.MEDIATOR)) {
            AthenaServer.commonJobScheduler
                .addOneTimeJob("mediator-killer-job", 10 * 60 * 1000, () -> {
                    if (downgradeTriggered.get()) {
                        return;
                    }
                    AthenaUtils.configLogbackToOldLog();
                    logger.error("DAL mediator doesn't exit within 6 mins, trigger downgrade");
                    this.startDowngrade();
                });
        }

        Signal.handle(new Signal("USR2"), signal -> {
            logger.error(AthenaUtils.pidStr() + " signal USR2 received!");
            MetricFactory.newCounter(Metrics.DAL_UPGRADE).addTag("Role", Constants.ROLE)
                .addTag(TraceNames.STATUS, String.valueOf(STATUS.SIGNAL_RECEIVED)).once();
            try {
                isUSR2SignalReceived.set(true);
                if (!isOSSupportReusePort()) {
                    logger.error(
                        AthenaUtils.pidStr() + " kernel does not support reuseport, exit directly");
                    exit();
                }

                if (!shouldStartSmoothUpgrade()) {
                    logger.error(AthenaUtils.pidStr()
                        + " athena smooth upgrade indicator file not found, exit directly");
                    exit();
                }

                logger.error(AthenaUtils.pidStr() + " start to downgrade listen");
                downgradeServer.start();
                // add job to start downgrade automatically in case not received command from shell
                AthenaServer.quickJobScheduler
                    .addOneTimeJob("UpgradeCenter_start_downgrade", Constants.UPGRADE_TIME_LIMIT,
                        () -> this.startDowngrade());
                AthenaUtils.configLogbackToOldLog();
                logger.error(AthenaUtils.pidStr() + " finish to config Logback to old log",
                    Constants.PID);
            } catch (Exception e) {
                logger.error("exc when handling signal ...", e);
                System.exit(1);
            }
        });
    }

    public boolean isDowngradeTriggered() {
        return isUSR2SignalReceived.get();
    }

    private boolean shouldStartSmoothUpgrade() throws InterruptedException {
        File[] files = new File("/data/run/dal_upgrade")
            .listFiles((dir, filename) -> filename.startsWith(Constants.UPGRADE_FLAG_FILE_PREFIX));
        if (files == null || files.length <= 0) {
            logger.error(AthenaUtils.pidStr() + "failed to find athena smooth upgrade flag file");
            return false;
        }

        if (!isUpgradeFileValid(files)) {
            logger
                .error(AthenaUtils.pidStr() + " cannot find valid athena smooth upgrade flag file");
            return false;
        }

        return true;
    }

    private boolean isOSSupportReusePort() {
        return Constants.REUSEPORT_SUPPORT_DEFAULT.equals(Constants.REUSEPORT_SUPPORT);
    }

    private boolean isUpgradeFileValid(File[] files) {
        Arrays.sort(files, LastModifiedFileComparator.LASTMODIFIED_COMPARATOR);

        File targetFile = files[files.length - 1];
        String[] parts = targetFile.getName().split("_");
        Long createdTime = Long.parseLong(parts[parts.length - 1]);
        return createdTime > System.currentTimeMillis() - Constants.UPGRADE_TIME_LIMIT;
    }

    private void exit() {
        NoThrow.call(() -> Thread.sleep(2000));
        System.exit(1);
    }
}
