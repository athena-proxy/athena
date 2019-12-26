package me.ele.jarch.athena.util.deploy.dalgroupcfg;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import me.ele.jarch.athena.allinone.AllInOneMonitor;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.constant.Metrics;
import me.ele.jarch.athena.constant.TraceNames;
import me.ele.jarch.athena.netty.AthenaServer;
import me.ele.jarch.athena.netty.upgrade.UpgradeCenter;
import me.ele.jarch.athena.scheduler.DBChannelDispatcher;
import me.ele.jarch.athena.util.CredentialsCfg;
import me.ele.jarch.athena.util.NoThrow;
import me.ele.jarch.athena.util.UserInfo;
import me.ele.jarch.athena.util.etrace.MetricFactory;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class DalGroupCfgFileLoader {
    private static final Logger logger = LoggerFactory.getLogger(DalGroupCfgFileLoader.class);

    public static final DalGroupCfgFileLoader LOADER = new DalGroupCfgFileLoader();

    private static final Map<String, String> DAL_GROUP_CFG_CACHE = new ConcurrentHashMap<>();
    private final Pattern PATTERN = Pattern.compile("dalgroup-(.*)\\.json$");

    private DalGroupCfgFileLoader() {
    }

    public boolean tryLoad() throws InterruptedException {
        Map<String, String> currentMD5Map = getLatestDalGroupMD5();
        offlineDeletedDalGroup(currentMD5Map.keySet());
        Set<String> changedDalgroups = currentMD5Map.entrySet().stream().filter(
            entry -> !entry.getValue()
                .equals(DalGroupCfgFileLoader.DAL_GROUP_CFG_CACHE.get(entry.getKey())))
            .map(Map.Entry::getKey).collect(Collectors.toSet());
        if (!changedDalgroups.isEmpty()) {
            // safely wait the file writing done.
            AthenaServer.commonJobScheduler.addOneTimeJob("dalGroupCfgFileLoaderJob", 2000, () -> {
                loadChangedDalGroups(changedDalgroups);
                DalGroupCfgFileLoader.DAL_GROUP_CFG_CACHE.putAll(currentMD5Map);
            });
            return true;
        }
        return false;
    }

    public void initDalGroupCfgs() {
        NoThrow.call(() -> {
            Thread.sleep(1000);
            DAL_GROUP_CFG_CACHE.putAll(getLatestDalGroupMD5());
            loadChangedDalGroups(DAL_GROUP_CFG_CACHE.keySet());
        });
    }

    public void checkFolderAccess() {
        Path dir = Paths.get(Constants.DALGROUP_CFG_FOLDER);
        if (!Files.exists(dir)) {
            try {
                Files.createDirectories(dir);
            } catch (IOException e) {
                logger.error("Got error when trying access dalgroup folder {}",
                    Constants.DALGROUP_CFG_FOLDER, e);
                throw new RuntimeException("Got error when trying access dalgroup folder "
                    + Constants.DALGROUP_CFG_FOLDER);
            }
        }
    }

    public void clearDalGroupCfgFiles() {
        NoThrow.call(() -> {
            File[] files = new File(Constants.DALGROUP_CFG_FOLDER).listFiles();
            for (File file : files != null ? files : new File[0]) {
                file.delete();
            }
        });
    }

    public void loadChangedDalGroups(Set<String> dalGroups) {
        long start = System.currentTimeMillis();
        //Load DUMMY prior to other dalgroups
        dalGroups.stream().filter(s -> s.contains("dalgroup-" + Constants.DUMMY + ".json"))
            .findFirst().ifPresent(dummy -> {
            loadDalGroupFile(dummy);
            dalGroups.remove(dummy);
        });
        dalGroups.forEach(this::loadDalGroupFile);
        logger.info(
            "Finished loading all changed dalgroup configs, dalgroup number is {}, time used {}",
            dalGroups.size(), System.currentTimeMillis() - start);
    }

    private void loadDalGroupFile(String path) {
        //if downgrade is triggered, stop loading any configuration changes
        if (UpgradeCenter.getInstance().isDowngradeTriggered()) {
            return;
        }
        logger.info("start to load dalgroup config for file {}", path);
        Path filePath = Paths.get(path);
        if (!Files.exists(filePath) || !Files.isReadable(filePath)) {
            logger.error("file doesn't exist or not readable", filePath.toString());
            return;
        }
        try {
            ObjectMapper objectMapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            DalGroupConfig dalGroupConfig =
                objectMapper.readValue(Files.newInputStream(filePath), DalGroupConfig.class);
            // Load seq db and other infos from DUMMY dalgroup
            if (Constants.DUMMY.equals(dalGroupConfig.getName())) {
                AllInOneMonitor.getInstance().updateSeqDbs(dalGroupConfig.getDbInfos());
                loadBackdoorUsers(dalGroupConfig);
                logger.info("Finished loaded  DUMMY dalgroup config for {}", path);
                return;
            }
            // for db heartbet and so on
            DBChannelDispatcher.updateDispatcher(dalGroupConfig);
            logger.info("Finished loaded dalgroup config for {}", path);
            MetricFactory.newCounter(Metrics.DALGROUP_CONFIG_LOAD)
                .addTag(TraceNames.DALGROUP, getDalgroupFromPath(path))
                .addTag(TraceNames.STATUS, "Success").once();
        } catch (Exception e) {
            logger.error("failed to load dalgroup configs from file {}", path, e);
            MetricFactory.newCounter(Metrics.DALGROUP_CONFIG_LOAD)
                .addTag(TraceNames.DALGROUP, getDalgroupFromPath(path))
                .addTag(TraceNames.STATUS, "Failed").once();
        }
    }

    private String getDalgroupFromPath(String path) {
        Matcher m = PATTERN.matcher(path);
        if (m.find()) {
            return m.group(1);
        }
        return "";
    }

    /**
     * username format: %username%readonly=true
     *
     * @param dalGroupConfig
     */
    private void loadBackdoorUsers(DalGroupConfig dalGroupConfig) {
        dalGroupConfig.getDbInfos().stream()
            .filter(dbConnectionInfo -> dbConnectionInfo.getUser().startsWith("%"))
            .forEach(dbInfo -> {
                String[] params = dbInfo.getUser().split("%");
                boolean isReadonly = false;
                if (params.length == 3 && Objects.equals(params[2], "readonly=true")) {
                    isReadonly = true;
                }
                UserInfo bdUser = new UserInfo(params[1], dbInfo.getEncodedPasswd(), isReadonly);
                CredentialsCfg.getConfig().put(params[1], bdUser);
            });
    }

    private Map<String, String> getLatestDalGroupMD5() {
        Map<String, String> currentTimestamp = new HashMap<>();
        File[] files = new File(Constants.DALGROUP_CFG_FOLDER).listFiles();
        if (Objects.isNull(files)) {
            logger.error("No files found under folder {}", Constants.DALGROUP_CFG_FOLDER);
            return Collections.emptyMap();
        }
        for (File f : files) {
            if (PATTERN.matcher(f.getAbsolutePath()).find()) {
                try (FileInputStream fis = new FileInputStream(f)) {
                    currentTimestamp.put(f.getAbsolutePath(), DigestUtils.md5Hex(fis));
                } catch (IOException e) {
                    logger.error("Getting error when reading file {}", f.getAbsolutePath(), e);
                }
            }
        }
        return currentTimestamp;
    }

    private void offlineDeletedDalGroup(Set<String> newDalGroupCfgs) {
        List<String> removedDalGroup =
            DAL_GROUP_CFG_CACHE.keySet().stream().filter(k -> !newDalGroupCfgs.contains(k))
                .collect(Collectors.toList());

        removedDalGroup.forEach(s -> {
            String dalGroup = getDalgroupFromPath(s);
            DBChannelDispatcher.getHolders().entrySet().stream()
                .filter(e -> e.getKey().equals(dalGroup)).findAny()
                .ifPresent(e -> e.getValue().clearResources());
            DAL_GROUP_CFG_CACHE.remove(s);
        });
    }
}
