package me.ele.jarch.athena.util;

import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.scheduler.DBChannelDispatcher;
import me.ele.jarch.athena.util.etrace.MetricFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CredentialsCfg {

    private static final Logger logger = LoggerFactory.getLogger(CredentialsCfg.class);

    /**
     * map中key为literalName, value为UserInfo对应dal-crendentials.cfg
     * 每行的实体映射
     *
     * @see UserInfo
     */
    private static volatile Map<String, UserInfo> config = new ConcurrentHashMap<>();

    public synchronized static void loadConfig(String filepath) {
        File file = new File(filepath);
        MetricFactory.newCounter("config.load").once();
        logger.info("Load credentials ...");
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            Map<String, UserInfo> newConfig = new ConcurrentHashMap<>();
            br.lines().filter(line -> !line.trim().isEmpty() && !line.trim().startsWith("#"))
                .map(line -> line.trim().split("\\s+")).forEach(lineGroup -> {
                if (lineGroup.length == 2) {
                    newConfig.put(lineGroup[0], new UserInfo(lineGroup[0], lineGroup[1]));
                } else if (lineGroup.length == 3) {
                    UserInfo userInfo = new UserInfo(lineGroup[0], lineGroup[1]);
                    String readOnly = KVChainParser.parse(lineGroup[2])
                        .getOrDefault(Constants.READ_ONLY, "false");
                    userInfo.setReadOnly(Boolean.valueOf(readOnly));
                    newConfig.put(lineGroup[0], userInfo);
                } else {
                    logger.warn(String
                        .format("invalided credential config line: %s in file: %s", lineGroup,
                            filepath));
                }
            });
            config = newConfig;
            //将credential更新至dalGroupConfig
            DBChannelDispatcher.TEMP_DAL_GROUP_CONFIGS
                .forEach((dalgroup, dalgroupCfg) -> dalgroupCfg.updateCredentials(dalgroup));
            if (logger.isDebugEnabled()) {
                logger.debug("new user crendentials: " + newConfig);
            }
        } catch (FileNotFoundException e) {
            MetricFactory.newCounter("config.error").once();
            logger.error("failed to find credential file.", e);
        } catch (IOException e) {
            MetricFactory.newCounter("config.error").once();
            logger.error("failed to load credential file.", e);
        }
        logger.info("Load credentials completed");
    }

    public static Map<String, UserInfo> getConfig() {
        return config;
    }
}
