package me.ele.jarch.athena.netty;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.etrace.agent.Trace;
import me.ele.jarch.athena.allinone.AllInOneMonitor;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.detector.DetectorDelegate;
import me.ele.jarch.athena.netty.debug.DebugHttpServer;
import me.ele.jarch.athena.netty.upgrade.UpgradeCenter;
import me.ele.jarch.athena.scheduler.HangSessionMonitor;
import me.ele.jarch.athena.scheduler.MulScheduler;
import me.ele.jarch.athena.scheduler.MulSchedulerMonitor;
import me.ele.jarch.athena.sharding.ShardingConfigFileReloader;
import me.ele.jarch.athena.util.*;
import me.ele.jarch.athena.util.config.GlobalIdConfig;
import me.ele.jarch.athena.util.curator.CuratorMonitorJob;
import me.ele.jarch.athena.util.deploy.OrgConfig;
import me.ele.jarch.athena.util.deploy.dalgroupcfg.DalGroupCfgFileLoader;
import me.ele.jarch.athena.util.health.check.DalGroupHealthCheck;
import me.ele.jarch.athena.util.log.DiscardAndTracePolicy;
import me.ele.jarch.athena.util.oomdetect.DirectOOMDetector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class AthenaServer {
    private static final Logger logger = LoggerFactory.getLogger(AthenaServer.class);

    public static final JobScheduler commonJobScheduler = new JobScheduler("commonJob");
    public static final JobScheduler quickJobScheduler = new JobScheduler("quickJob");
    public static final JobScheduler vitalJobScheduler = new JobScheduler("vitalJob");
    public static final JobScheduler zkJobScheduler = new JobScheduler("zkJob");
    public static final JobScheduler rmqJobScheduler = new JobScheduler("rmqJob");
    public static final JobScheduler dbDelayJobScheduler = new JobScheduler("dbDelayJob");
    public static final JobScheduler slowDetectJobScheduler = new JobScheduler("slowDetectJob");
    public static final JobScheduler dalGroupHealthCheckJobScheduler =
        new JobScheduler("dalGroupHealthCheckJob");
    public static final JobScheduler dalConfTableCheckJobScheduler =
        new JobScheduler("dalConfTableCheckJob");
    public static final JobScheduler letItGoJobScheduler = new JobScheduler("letItGoJob");
    public static final ExecutorService DANGER_SQL_FILTER_EXECUTOR =
        new ThreadPoolExecutor(1, 2, 10, TimeUnit.MINUTES, new ArrayBlockingQueue<>(5120),
            new ThreadFactoryBuilder().setNameFormat("danger-pool-%d").build(),
            new DiscardAndTracePolicy());

    public static final GlobalZKCache globalZKCache = new GlobalZKCache(Constants.APPID);

    private static DebugHttpServer debugHttpServer;

    private static boolean perfectStart = true;
    private static Optional<String> errorOp = Optional.empty();

    public static boolean isPerfectStart() {
        return perfectStart;
    }

    public static String startError() {
        return errorOp.orElse("started perfectly");
    }

    public static void main(String[] args) {
        try {
            init();
        } catch (Throwable t) {
            perfectStart = false;
            errorOp = Optional.of(t.getMessage());
            logger.error("Start AthenaServer got error", t);
            Trace.logError(t);
        }
    }

    private static void init() throws Throwable {
        for (int i = 0; i < 10; i++) {
            logger
                .info(AthenaUtils.pidStr() + " ##################################", Constants.PID);
        }
        logger.info("Athena is ready to startup ...");
        AthenaConfig.getInstance().load();
        // 在athena.properties配置加载完成后马上初始化AthenaEventLoopGroupCenter,该操作需优先于数据库连接
        AthenaEventLoopGroupCenter.init(AthenaConfig.getInstance().getNettyThreadCount(),
            AthenaConfig.getInstance().getAsyncThreadCount());
        ShardingConfigFileReloader.loader
            .setShrdingConfigFile(AthenaConfig.getInstance().getShardingConfigPath());
        logger.info("Startup job Scheduler ....");
        commonJobScheduler.start();
        quickJobScheduler.start();
        vitalJobScheduler.start();
        zkJobScheduler.start();
        rmqJobScheduler.start();
        dbDelayJobScheduler.start();
        slowDetectJobScheduler.start();
        dalGroupHealthCheckJobScheduler.start();
        dalConfTableCheckJobScheduler.start();
        letItGoJobScheduler.start();

        //check dalgroupConfig file folder accessibility
        DalGroupCfgFileLoader.LOADER.checkFolderAccess();

        logger.info("Run PreLoad ....");
        //Run before start config file monitors and before bind the port
        PreLoad.getInstance().load();

        logger.info("Initialize config file monitors ....");
        initConfigMonitors();

        StartupMisc.doPrepareStartup();


        AthenaFrontServer.getInstance().bindFrontPorts();

        AthenaServer.debugHttpServer = new DebugHttpServer();
        debugHttpServer.startDynamicServer(Constants.APPID);
        commonJobScheduler.addJob("restore_fixed_debug_server", 30 * 1000,
            () -> debugHttpServer.tryStartFixedServer());

        if (isManualRelease()) {
            logger.info(
                AthenaUtils.pidStr() + " manual release,no need to register signal handler ....");
        } else {
            /*
             * After binding port and debug server startup, we must trigger our upgrade process, to notify our existence.
             */
            UpgradeCenter.getInstance().startUpgrade();
            logger.info(AthenaUtils.pidStr() + " start to register signal handler ....");
            UpgradeCenter.getInstance().registerSignalHandler();
        }
        quickJobScheduler.addJob("monitor_hang_commit", 2000,
            () -> HangSessionMonitor.getInst().scanHangSessions());
        logger.info(AthenaUtils.pidStr() + " finished startup server ...");
        // send dalgroup health status to etrace
        dalGroupHealthCheckJobScheduler.addOneTimeJob("start_dal_group_health_check", 3 * 60 * 1000,
            () -> dalGroupHealthCheckJobScheduler.addOneTimeJob("dal_group_health_check",
                GreySwitch.getInstance().getHealthCheckInterval(),
                DalGroupHealthCheck.getInstance()::check));
        initNettyMetrics();
        initMulSchedulerMetrics();
        initCuratorMonitor();
        //add job to detect direct memory oom
        DirectOOMDetector.getInstance().addDetectJob();
    }

    public static boolean isManualRelease() {
        return Constants.MANUAL_RELEASE_DEFAULT.equals(Constants.MANUAL_RELEASE);
    }

    private static void initConfigMonitors() {
        if (!GreySwitch.getInstance().isOldConfigFileDisabled()) {
            commonJobScheduler.addJob("sharding_load_config", 5 * 1000,
                () -> ShardingConfigFileReloader.loader.tryLoad());

            String allInOneFile = AthenaConfig.getInstance().getAllInOneFile();
            AllInOneMonitor allInOneMonitor = AllInOneMonitor.getInstance();

            ConfigFileReloader allInOneReloader = new ConfigFileReloader(allInOneFile,
                i -> AllInOneMonitor.getInstance().loadConfig(i));
            commonJobScheduler
                .addJob("all_in_one_load_config", 5 * 1000, allInOneReloader::tryLoad);

            commonJobScheduler.addJob("all_in_one_clean_conn", 30 * 1000,
                () -> allInOneMonitor.cleanInactiveDBConnection());

            String credentialFile = AthenaConfig.getInstance().getCredentialFile();

            ConfigFileReloader credentialReloader =
                new ConfigFileReloader(credentialFile, i -> CredentialsCfg.loadConfig(i));
            commonJobScheduler
                .addJob("credential_config", 5 * 1000, () -> credentialReloader.tryLoad());

            String portFile = AthenaConfig.getInstance().getAdditionalPortFile();
            ConfigFileReloader additionalPortReloader = new ConfigFileReloader(portFile,
                i -> DBChannelCfgMonitor.getInstance().tryLoadAndRestart(i));
            commonJobScheduler.addJob("additional_dal_proxy_port", 5 * 1000,
                () -> additionalPortReloader.tryLoad());

            String orgFile = AthenaConfig.getInstance().getOrgFile();
            ConfigFileReloader orgReloader =
                new ConfigFileReloader(orgFile, i -> OrgConfig.getInstance().loadConfig(orgFile));
            commonJobScheduler
                .addJob("load_goproxy_front_port", 5 * 1000, () -> orgReloader.tryLoad());

            String globalIdsFile = AthenaConfig.getInstance().getGlobalIdsConfigPath();
            ConfigFileReloader globalIdsReloader =
                new ConfigFileReloader(globalIdsFile, i -> GlobalIdConfig.loadCfg(i));
            commonJobScheduler
                .addJob("globalIds_config", 5 * 1000, () -> globalIdsReloader.tryLoad());
        }

        String greySwitchFile = AthenaConfig.getInstance().getGreySwitchConfigPath();
        ConfigFileReloader greySwitchReloader =
            new ConfigFileReloader(greySwitchFile, i -> GreySwitchCfg.tryLoadCfg(greySwitchFile));
        commonJobScheduler
            .addJob("grey_switch_config", 5 * 1000, () -> greySwitchReloader.tryLoad());

        String credentialFile = AthenaConfig.getInstance().getCredentialFile();
        ConfigFileReloader credentialReloader =
            new ConfigFileReloader(credentialFile, i -> CredentialsCfg.loadConfig(i));
        commonJobScheduler
            .addJob("credential_config", 5 * 1000, () -> credentialReloader.tryLoad());

        commonJobScheduler.addJob("dalgroup_cfg", 5_000, DalGroupCfgFileLoader.LOADER::tryLoad);

        slowDetectJobScheduler
            .addJob("slow_detector", 10 * 1000, () -> DetectorDelegate.startDetect());

    }

    static void initNettyMetrics() {
        Monitorable clientNettyMonitorJob =
            new EventLoopGroupMonitorJob(AthenaEventLoopGroupCenter.getClientWorkerGroup(),
                "client");
        commonJobScheduler
            .addJob("client_netty_task_metric", 2 * 1000, clientNettyMonitorJob::monitor);
        Monitorable serverNettyMonitorJob =
            new EventLoopGroupMonitorJob(AthenaEventLoopGroupCenter.getSeverWorkerGroup(),
                "server");
        commonJobScheduler
            .addJob("server_netty_task_metric", 2 * 1000, serverNettyMonitorJob::monitor);
        Monitorable asyncNettyMonitorJob =
            new EventLoopGroupMonitorJob(AthenaEventLoopGroupCenter.getAsyncWorkerGroup(), "async");
        commonJobScheduler
            .addJob("async_netty_task_metric", 2 * 1000, asyncNettyMonitorJob::monitor);
        Monitorable debugNettyMonitorJob =
            new EventLoopGroupMonitorJob(AthenaEventLoopGroupCenter.getDebugServerGroup(), "debug");
        commonJobScheduler
            .addJob("debug_netty_task_metric", 2 * 1000, debugNettyMonitorJob::monitor);
    }

    static void initMulSchedulerMetrics() {
        Monitorable mulSchedulerMonitor = new MulSchedulerMonitor(MulScheduler.getInstance());
        commonJobScheduler.addJob("mulscheduler_registered_queues_task_metric", 2 * 1000,
            mulSchedulerMonitor::monitor);
    }

    static void initCuratorMonitor() {
        CuratorMonitorJob curatorMonitorJob = new CuratorMonitorJob();
        commonJobScheduler.addJob("curator_monitor", 300 * 1000, curatorMonitorJob::monitor);
    }
}
