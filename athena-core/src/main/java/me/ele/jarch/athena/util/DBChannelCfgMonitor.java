package me.ele.jarch.athena.util;

import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.scheduler.DBChannelDispatcher;
import me.ele.jarch.athena.util.deploy.OrgConfig;
import me.ele.jarch.athena.util.deploy.dalgroupcfg.DalGroupConfig;
import me.ele.jarch.athena.util.etrace.MetricFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Created by donxuu on 8/18/15.
 */
public class DBChannelCfgMonitor {
    private static final Logger logger = LoggerFactory.getLogger(DBChannelCfgMonitor.class);
    private Set<String> dbChannelCfgs = new CopyOnWriteArraySet<>();
    private boolean isErrorLogWrited = false;


    private static class INNER {
        private static final DBChannelCfgMonitor monitor = new DBChannelCfgMonitor();
    }

    public static DBChannelCfgMonitor getInstance() {
        return INNER.monitor;
    }

    /**
     * This method detect if there's new proxy ports and restart the server. It checks if there's an additional file, dal-proxy-ports.cfg exits. load and
     * restart the server. Note, this method does not really restart the server. it just force the server process quit. then the supervisor will restore the
     * process. This may not work in test environment.
     */
    public void tryLoadAndRestart(String additionalConfigFile) {
        Set<String> dbChannelCfgSet = getAllCfgs(additionalConfigFile);
        Set<String> chgdDBChannelCfgs = new HashSet<>();
        dbChannelCfgSet.forEach(newValue -> {
            if (!dbChannelCfgs.contains(newValue)) {
                dbChannelCfgs.add(newValue);
                chgdDBChannelCfgs.add(newValue);
            }
        });
        if (!chgdDBChannelCfgs.isEmpty()) {
            AthenaConfig.getInstance().setDbCfgs(dbChannelCfgs);
            AthenaConfig.getInstance()
                .setDatabaseConnectString(buildDatabaseConnectString(dbChannelCfgs));
            DBChannelDispatcher
                .updateDispatchers(chgdDBChannelCfgs, AthenaConfig.getInstance().getConfig());
        }
        //offline those org/dalgroups removed from dal-proxy-ports.cfg
        dbChannelCfgs.stream().filter(s -> !dbChannelCfgSet.contains(s)).forEach(org -> {
            OrgConfig.getInstance().getDalGroupsByOrg(org).forEach(dalgroup -> {
                DalGroupConfig tempCfg =
                    DBChannelDispatcher.TEMP_DAL_GROUP_CONFIGS.get(dalgroup.getName());
                if (Objects.nonNull(tempCfg)) {
                    tempCfg.setDalgroupOffline(true);
                    tempCfg.deleteFile();
                }
            });
        });
        dbChannelCfgs = dbChannelCfgSet;
        // when delete org/dalgroup，update dbcfgs too
        if (!dbChannelCfgs.equals(AthenaConfig.getInstance().getDbCfgs())) {
            AthenaConfig.getInstance().setDbCfgs(dbChannelCfgs);
            AthenaConfig.getInstance()
                .setDatabaseConnectString(buildDatabaseConnectString(dbChannelCfgs));
        }
    }

    public void initDispatchers() {
        dbChannelCfgs.addAll(getAllCfgs(AthenaConfig.getInstance().getAdditionalPortFile()));
        AthenaConfig.getInstance()
            .setDatabaseConnectString(buildDatabaseConnectString(dbChannelCfgs));
        AthenaConfig.getInstance().setDbCfgs(dbChannelCfgs);
        DBChannelDispatcher
            .updateDispatchers(dbChannelCfgs, AthenaConfig.getInstance().getConfig());
    }

    public Set<String> getAllCfgs(String additionalConfigFile) {
        String additionalConnString = loadFromSeparateFile(additionalConfigFile);
        if (additionalConnString.isEmpty()) {
            return new HashSet<>();
        }
        return getCfgs(additionalConnString);
    }

    public String buildDatabaseConnectString(Set<String> dbChannelCfgs) {
        if (dbChannelCfgs == null || dbChannelCfgs.isEmpty())
            return "";
        StringBuffer sb = new StringBuffer();
        dbChannelCfgs.forEach(s -> sb.append(s).append(","));
        return sb.substring(0, sb.length() - 1);
    }

    private String loadFromSeparateFile(String additionalPortConfigFile) {
        File file = new File(additionalPortConfigFile);
        if (!file.exists()) {
            if (!isErrorLogWrited) {
                logger.error(String.format("The ports config file, %s, does not exist.",
                    additionalPortConfigFile));
                isErrorLogWrited = true;
            }
            return "";
        }

        if (!file.canRead()) {
            logger.error(String.format("The ports config file, %s, exists but can not read.",
                additionalPortConfigFile));
            return "";
        }
        logger.info("Try to load additional file : " + additionalPortConfigFile.toString());

        StringBuilder builder = new StringBuilder();
        try (BufferedReader br = new BufferedReader(
            new InputStreamReader(new FileInputStream(additionalPortConfigFile)))) {
            br.lines().filter(line -> !line.trim().isEmpty() && !line.trim().startsWith("#"))
                .map(line -> line.trim().split("\\s+")).filter(lineGroup -> lineGroup.length == 3)
                .filter(lineGroup -> lineGroup[0].equals(Constants.APPID) && lineGroup[1]
                    .equals(Constants.HOSTNAME))
                .forEach(lineGroup -> builder.append(lineGroup[2]).append(","));
        } catch (Exception e) {
            logger.error("Failed to load additional dal groups.", e);
        }
        logger.info("The next is ports config file newest content: " + builder.toString());

        return builder.toString();

    }

    private Set<String> getCfgs(String configStr) {
        Set<String> groupSet = new HashSet<>();
        try {
            if (configStr == null) {
                MetricFactory.newCounter("config.error").once();
                logger.error("input dbChannelCfg parameter is null");
                return groupSet;
            }
            configStr = configStr.replaceAll("\\s+", "");
            String[] cfgs = configStr.split(",");
            if (cfgs.length == 0) {
                MetricFactory.newCounter("config.error").once();
                logger.error(String.format("db connection config is invalid : \"%s\"", configStr));
                return groupSet;
            }
            // format:
            // groupName
            for (String cfg : cfgs) {
                if (cfg == null || cfg.isEmpty()) {
                    continue;
                }
                groupSet.add(cfg);
            }
            return groupSet;
        } catch (Exception e) {
            logger.error("failed to parse config", e);
            return groupSet;
        }
    }
}
