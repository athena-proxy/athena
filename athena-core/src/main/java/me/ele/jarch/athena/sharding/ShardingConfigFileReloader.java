package me.ele.jarch.athena.sharding;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ShardingConfigFileReloader {
    public static final ShardingConfigFileReloader loader = new ShardingConfigFileReloader();
    private static final Logger logger = LoggerFactory.getLogger(ShardingConfigFileReloader.class);
    private final Map<String, Long> configFiles = new ConcurrentHashMap<>();
    private String shrdingConfigFile = "";

    public ShardingConfigFileReloader() {
    }

    public boolean tryLoad() throws InterruptedException {
        Map<String, Long> currentTimestamp = new HashMap<>();
        for (String configFile : configFiles.keySet()) {
            File file = new File(configFile);
            currentTimestamp.put(configFile, file.lastModified());
        }

        boolean ischange = currentTimestamp.keySet().stream().
            anyMatch(configFile -> !currentTimestamp.get(configFile)
                .equals(configFiles.get(configFile)));
        if (ischange) {
            Thread.sleep(2000); // safely wait the file writing failed.
            loadShardingConfig();
            configFiles.putAll(currentTimestamp);
            return true;
        }

        return false;
    }

    public synchronized void loadShardingConfig() {
        try {
            File file = new File(shrdingConfigFile);
            if (!file.exists()) {
                return;
            }
            ShardingConfig.load(shrdingConfigFile);
            ShardingConfig.initShardingCfg();
        } catch (Exception t) {
            logger.error("failed to load Sharding configuration file.", t);
        }
    }

    public void addConfigFile(String configFile) {
        this.configFiles.computeIfAbsent(configFile, (file) -> new File(file).lastModified());
    }


    public String getShrdingConfigFile() {
        return shrdingConfigFile;
    }

    public void setShrdingConfigFile(String shrdingConfigFile) {
        this.shrdingConfigFile = shrdingConfigFile;
        this.configFiles.put(shrdingConfigFile, -1L);
    }
}
