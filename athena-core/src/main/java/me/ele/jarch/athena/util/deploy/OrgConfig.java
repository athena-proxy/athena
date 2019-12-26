package me.ele.jarch.athena.util.deploy;

import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.scheduler.DBChannelDispatcher;
import me.ele.jarch.athena.util.AthenaConfig;
import me.ele.jarch.athena.util.KVChainParser;
import org.apache.commons.io.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class OrgConfig {
    private static final Logger logger = LoggerFactory.getLogger(OrgConfig.class);
    private static OrgConfig instance = new OrgConfig();
    private volatile Map<String, List<DALGroup>> dalGroupsMap = new HashMap<>();
    private boolean isWarnLogWrited = false;

    public static OrgConfig getInstance() {
        return instance;
    }

    private OrgConfig() {

    }

    public synchronized void loadConfig(String filepath) {
        File file = new File(filepath);
        if (!file.exists()) {
            if (!isWarnLogWrited) {
                logger.warn(String.format("org config file %s does not exist.", filepath));
                isWarnLogWrited = true;
            }
            return;
        }
        logger.warn("load OrgConfig from " + filepath);
        Map<String, List<DALGroup>> newdalGroupsMap = new HashMap<>();
        Set<String> newDalGroupNames = new HashSet<>();
        AtomicInteger lineNumber = new AtomicInteger(0);
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(filepath), Charsets.UTF_8)) {
            reader.lines().forEachOrdered(line -> {
                lineNumber.incrementAndGet();
                String trimLine = line.trim();
                if (trimLine.trim().startsWith("#")) {
                    return;
                }
                if (trimLine.trim().isEmpty()) {
                    return;
                }
                try {
                    String[] splitedLine = trimLine.split("\\s+", 4);
                    //format: org frontport dalgroup attributes
                    if (splitedLine.length < 3) {
                        logger
                            .error("Invalid org config line:" + lineNumber.get() + " " + trimLine);
                        return;
                    }
                    String org = splitedLine[0];
                    String groupname = splitedLine[2];
                    String attributesString = splitedLine.length > 3 ? splitedLine[3] : "";
                    Map<String, String> attributes = parseAttributes(attributesString);
                    boolean batchSwitchAllowed =
                        "true".equals(attributes.get(Constants.BATCH_SWITCH_NAME));
                    boolean pureSlaveOnly =
                        "true".equals(attributes.get(Constants.PURE_SLAVE_ONLY));
                    String slaveSelectStrategy =
                        attributes.getOrDefault(Constants.SLAVE_SELECT_STRATEGY, "default");
                    String dbgroup = attributes.getOrDefault("db", groupname);
                    DALGroup group = new DALGroup(groupname, org, dbgroup);
                    group.setBatchAllowed(batchSwitchAllowed);
                    group.setPureSlaveOnly(pureSlaveOnly);
                    group.setSlaveSelectStrategy(slaveSelectStrategy);
                    newdalGroupsMap.computeIfAbsent(org, k -> new ArrayList<>());
                    newdalGroupsMap.get(org).add(group);
                    newDalGroupNames.add(groupname);
                } catch (Exception e) {
                    logger.error("Failed to parse line:" + lineNumber.get() + " " + trimLine, e);
                }
            });
        } catch (IOException e) {
            logger.error("error to load dal org config file:" + filepath, e);
            return;
        }
        this.dalGroupsMap = newdalGroupsMap;
        DBChannelDispatcher.updateDispatchers(AthenaConfig.getInstance().getDbCfgs(),
            AthenaConfig.getInstance().getConfig());
    }

    private Map<String, String> parseAttributes(String string) {
        return KVChainParser.parse(string);
    }

    public List<DALGroup> getDalGroupsByOrg(String org) {
        return dalGroupsMap.getOrDefault(org, new ArrayList<>());
    }
}
