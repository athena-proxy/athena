package me.ele.jarch.athena.netty;

import me.ele.jarch.athena.allinone.AllInOneMonitor;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.constant.Metrics;
import me.ele.jarch.athena.constant.TraceNames;
import me.ele.jarch.athena.sharding.ShardingConfigFileReloader;
import me.ele.jarch.athena.util.*;
import me.ele.jarch.athena.util.config.GlobalIdConfig;
import me.ele.jarch.athena.util.curator.ZkCurator;
import me.ele.jarch.athena.util.deploy.OrgConfig;
import me.ele.jarch.athena.util.deploy.dalgroupcfg.DalGroupCfgFileLoader;
import me.ele.jarch.athena.util.etrace.MetricFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class PreLoad {
    private static final Logger logger = LoggerFactory.getLogger(PreLoad.class);


    private static class LazyHolder {
        private static final PreLoad INSTANCE = new PreLoad();
    }

    public static PreLoad getInstance() {
        return LazyHolder.INSTANCE;
    }

    private PreLoad() {

    }

    public boolean load() throws InterruptedException, IOException {
        logger.info("Preload zk caches ....");
        CountDownLatch zkInitLatch = new CountDownLatch(1);
        ZkCurator.initCuratorBlock(zkInitLatch);

        CountDownLatch preloadInitLatch = new CountDownLatch(1);

        logger.info("Preload dal-grey-switch.cfg ....");
        GreySwitchCfg.tryLoadCfg(AthenaConfig.getInstance().getGreySwitchConfigPath());

        logger.info("Preload dal-credentials.cfg ....");
        CredentialsCfg.loadConfig(AthenaConfig.getInstance().getCredentialFile());

        if (!GreySwitch.getInstance().isOldConfigFileDisabled()) {
            logger.info("Preload goproxy-front-port.cfg ....");
            OrgConfig.getInstance().loadConfig(AthenaConfig.getInstance().getOrgFile());
            logger.info("Preload sharding.yml ....");
            ShardingConfigFileReloader.loader.loadShardingConfig();
            logger.info("Preload db-connections.cfg ....");
            AllInOneMonitor.getInstance().loadConfig(AthenaConfig.getInstance().getAllInOneFile());
            logger.info("Preload dal-credentials.cfg ....");
            CredentialsCfg.loadConfig(AthenaConfig.getInstance().getCredentialFile());
            logger.info("Preload dal-proxy-ports.cfg ....");
            DBChannelCfgMonitor.getInstance().initDispatchers();
            logger.info("Preload dal-globalids.yml ...");
            GlobalIdConfig.loadCfg(AthenaConfig.getInstance().getGlobalIdsConfigPath());
        }
        //需要在灰度开关加载完之后加载dalgroup配置
        logger.info("Preload dalgroup configs ...");
        DalGroupCfgFileLoader.LOADER.initDalGroupCfgs();

        if (!AthenaServer.isManualRelease()) {
            /**
             * GZS和DAL并行启动，sleep时间固定成DB心跳完成时间,加3s方便tail -f
             */
            long sleepTime = Constants.STARTUP_DELAY_TIME + 3000;
            logger.info(AthenaUtils.pidStr() + " waiting " + sleepTime
                + "ms for athena to be ready before binding to ports ....");
            Thread.sleep(sleepTime);
        }
        //wait until ZK cache initialized
        if (!zkInitLatch.await(5, TimeUnit.MINUTES)) {
            logger.error("cache was not initialed in 60,000 ms");
            MetricFactory.newCounter(Metrics.ZK_CACHE).addTag(TraceNames.STATUS, "TIMEOUT").once();
        }
        preloadInitLatch.countDown();
        return true;
    }
}
