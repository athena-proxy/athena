package me.ele.jarch.athena.util;

import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.detector.DetectorDelegate;
import me.ele.jarch.athena.util.etrace.MetricFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Map;

/**
 * 用于所有灰度上线时的开关配置
 * <p>
 * 该含义和订单表的灰度sharding没有关系, 为了和之前的GrayUp区别, 所以取名Grey而非Gray
 */
public class GreySwitchCfg {
    private static final Logger logger = LoggerFactory.getLogger(GreySwitchCfg.class);

    public synchronized static void tryLoadCfg(String filePath) {
        File file = new File(filePath);
        if (!file.exists()) {
            return;
        }
        if (!file.canRead()) {
            logger.warn("greyswitch config file: {} could not be read", filePath);
            return;
        }

        logger.info("greyswitch config file loading...");
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            br.lines().map(line -> line.trim())
                .filter(line -> !line.isEmpty() && !line.startsWith("#")).forEach(line -> {
                String[] kv = line.split(" ");
                if (kv.length != 2)
                    return;
                if ("global".equals(kv[0]) || Constants.HOSTNAME.equals(kv[0])) {
                    handleGlobalCfg(kv[1]);
                }
            });
        } catch (Exception e) {
            logger.error("some thing error occured when load desensitization config file", e);
        }
        logger.info("greyswitch config file load complete");
    }

    private static void handleGlobalCfg(String value) {
        Map<String, String> kvs = KVChainParser.parse(value);
        kvs.forEach((k, v) -> {
            try {
                switch (k) {
                    case Constants.GLOBALID_PARTITION_SWITCH:
                        GreySwitch.getInstance().setAllowGlobalIdPartition(Boolean.valueOf(v));
                        break;
                    case Constants.SEND_AUDIT_SQL_TO_RMQ_SWITCH:
                        GreySwitch.getInstance().setAllowSendAuditSqlToRmq(Boolean.valueOf(v));
                        break;
                    case Constants.OVERFLOW_TRACE_FREQUENCY:
                        GreySwitch.getInstance().setOverflowTraceFrequency(Integer.parseInt(v));
                        break;
                    case Constants.DETECT_SLOW_SWITCH:
                        DetectorDelegate.turnOn(Boolean.valueOf(v));
                        break;
                    case Constants.SMART_AK_OPEN:
                        GreySwitch.getInstance().setSmartAutoKillerOpen(Boolean.valueOf(v));
                        break;
                    case Constants.CLIENT_LOGIN_TIMEOUT_IN_MILLS:
                        GreySwitch.getInstance().setLoginTimeoutInMills(Long.parseLong(v));
                        break;
                    case Constants.CLIENT_LOGIN_SLOW_IN_MILLS:
                        GreySwitch.getInstance().setLoginSlowInMills(Long.parseLong(v));
                        break;
                    case Constants.SEND_ETRACE_METRIC_SWITCH:
                        MetricFactory.setEnable(Boolean.valueOf(v));
                        break;
                    case Constants.DAL_GROUP_HEALTH_CHECK_SWITCH:
                        GreySwitch.getInstance().setAllowDalGroupHealthCheck(Boolean.valueOf(v));
                        break;
                    case Constants.MAX_SQL_PATTERN_LENGTH:
                        GreySwitch.getInstance().setMaxSqlPatternLength(Integer.valueOf(v));
                        break;
                    case Constants.MHA_SLAVE_FAIL_SWITCH:
                        GreySwitch.getInstance().setMhaSlaveFailedEnabed(Boolean.valueOf(v));
                        break;
                    case Constants.DALGROUP_CFG_SWITCH:
                        GreySwitch.getInstance().setDalGroupCfgEnabled(Boolean.valueOf(v));
                        break;
                    case Constants.OLD_CONFIGFILE_DISABLED:
                        GreySwitch.getInstance().setOldConfigFileDisabled(Boolean.valueOf(v));
                        break;
                    case Constants.ZK_DALGROUP_WATCHER:
                        GreySwitch.getInstance().setZkDalgroupWatcherEnabled(Boolean.valueOf(v));
                        break;
                    case Constants.MAX_SQL_LENGTH:
                        GreySwitch.getInstance().setMaxSqlLength(Long.valueOf(v));
                        break;
                    case Constants.QUERY_SQL_LOG_SWITCH:
                        GreySwitch.getInstance().setQuerySqlLogEnabled(Boolean.valueOf(v));
                        break;
                    case Constants.MAX_RESPONSE_LENGTH:
                        GreySwitch.getInstance().setMaxResponseLength(Long.valueOf(v));
                        break;
                    case Constants.HEALTCH_CHECK_INTERVAL:
                        GreySwitch.getInstance().setHealthCheckInterval(Integer.valueOf(v));
                        break;
                    case Constants.WARM_UP_SESSION_COUNT:
                        GreySwitch.getInstance().setWarmUpConnectionCount(Integer.valueOf(v));
                        break;
                    case Constants.SHARDING_HASH_CHECK_LEVEL:
                        GreySwitch.getInstance().setShardingHashCheckLevel(v);
                        break;
                    case Constants.IS_SPECIFY_DB:
                        GreySwitch.getInstance().setSpecifyDb(Boolean.valueOf(v));
                        break;
                    case Constants.DANGER_SQL_FILTER_ENABLED:
                        GreySwitch.getInstance().setDangerSqlFilterEnabled(Boolean.valueOf(v));
                        break;
                    default:
                        return;
                }
                logger.info("[{}] has been set to [{}]", k, v);
            } catch (Exception e) {
                logger.error("failed to parse value for " + value, e);
            }
        });
    }
}
