package me.ele.jarch.athena.util.curator;

import me.ele.jarch.athena.constant.Metrics;
import me.ele.jarch.athena.constant.TraceNames;
import me.ele.jarch.athena.util.AthenaConfig;
import me.ele.jarch.athena.util.etrace.MetricFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.retry.RetryNTimes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ZkCurator {
    private static final Logger logger = LoggerFactory.getLogger(ZkCurator.class);

    private static volatile TreeCache cache = null;
    private static volatile CuratorFramework client = null;
    private static volatile String zookeeperPath = "";
    private static volatile boolean isInitialized = false;

    private static final int CONNECTION_TIMEOUT_MILLI = 10_000;
    private static final int SESSION_TIMEOUT_MILLI = 30_000;
    private static final int RETRY_INTERVAL_MILLI = 2000_000;
    private static final int BLOCK_TIMEOUT_MILLI = 12_000;

    private static volatile ZKCacheListener zkCacheListener;

    public static void initCuratorBlock(CountDownLatch initLatch) {
        initCurator();

        try {
            cache.getListenable().addListener(new CacheInitlizedListener(initLatch));

            if (!client.blockUntilConnected(BLOCK_TIMEOUT_MILLI, TimeUnit.MILLISECONDS)) {
                logger
                    .error("failed to connect to zookeer in {}, zk url is {}", BLOCK_TIMEOUT_MILLI,
                        zookeeperPath);
                MetricFactory.newCounter(Metrics.ZK_CONNECTION).addTag(TraceNames.STATUS, "FAILED")
                    .once();
                initLatch.countDown();
                MetricFactory.newCounter(Metrics.ZK_CACHE).addTag(TraceNames.STATUS, "FAILED")
                    .once();
            }
        } catch (Exception e) {
            logger.error("Got error while waiting for curator to start, exception is", e);
        }
    }

    public static synchronized void initCurator() {
        try {
            logger.info("start to initializing Curator");
            MetricFactory.newCounter(Metrics.ZK_CONNECTION)
                .addTag(TraceNames.STATUS, "INITIALIZING").once();
            zookeeperPath = AthenaConfig.getInstance().getZookeeperPath();

            validateZkPath(zookeeperPath);

            String connectPath = AthenaConfig.getInstance().getConnectPath();
            client = CuratorFrameworkFactory.builder().connectString(connectPath)
                .retryPolicy(new RetryNTimes(-1, RETRY_INTERVAL_MILLI))
                .connectionTimeoutMs(CONNECTION_TIMEOUT_MILLI)
                .sessionTimeoutMs(SESSION_TIMEOUT_MILLI).build();
            client.getUnhandledErrorListenable().addListener((message, e) -> logger
                .error("Got Exception when starting curator client, error messages is {}", message,
                    e));
            client.start();

            cache = TreeCache.newBuilder(client, zookeeperPath).setMaxDepth(4).build();
            cache.start();
            zkCacheListener = new ZKCacheListener();
            cache.getListenable().addListener(zkCacheListener);
        } catch (Exception e) {
            logger.error("Error initializing curator curator", e);
        }
    }

    public static Map<String, ChildData> getAllChildrenData(String dalgroup) {
        if (Objects.isNull(cache)) {
            return null;
        }
        return cache.getCurrentChildren(getFullPath(dalgroup));
    }

    public static boolean isConnected() {
        return isInitialized;
    }

    private static String getFullPath(String node) {
        return zookeeperPath.endsWith("/") ? zookeeperPath + node : zookeeperPath + "/" + node;
    }

    public static void setInitialized(boolean initialized) {
        isInitialized = initialized;
    }

    /**
     * zk path 需以 "/" 开头
     *
     * @param path
     */
    private static void validateZkPath(String path) {
        if (Objects.isNull(path) || !path.startsWith("/")) {
            logger.error("zookeeperPath [{}] is not in proper format, it should start with '/'",
                path);
        }
    }

    public static void close() {
        if (Objects.nonNull(cache)) {
            TreeCache tempCache = cache;
            cache = null;
            try {
                tempCache.close();
            } catch (Exception e) {
                logger.error("Error while close tree cache", e);
            }
        }

        if (Objects.nonNull(client)) {
            CuratorFramework tempClient = client;
            client = null;
            try {
                tempClient.close();
            } catch (Exception e) {
                logger.error("Error while close curator client", e);
            }
        }
    }
}
