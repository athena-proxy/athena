package me.ele.jarch.athena.util;

import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.util.rmq.SendThreshold;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by jinghao.wang on 16/3/21.
 */
public class GlobalZKCache extends ZKCache {
    private static final Logger logger = LoggerFactory.getLogger(GlobalZKCache.class);

    private volatile int fusePassRate = 4;
    private volatile int fuseWindowMillis = 2000;
    private volatile int smokeWindowMillis = 2000;
    private volatile int etraceSampleRate = 100;
    private volatile boolean allowShadowdb = false;
    private volatile String heartbeat;

    public GlobalZKCache(String groupName) {
        super(groupName);
    }

    @Override public void setZKProperty(Properties _config) {
        super.setZKProperty(_config);
        try {
            fusePassRate = Integer.valueOf(_config.getProperty(Constants.FUSE_PASS_RATE).trim());
            fuseWindowMillis =
                Integer.valueOf(_config.getProperty(Constants.FUSE_WINDOW_MILLIS).trim());
            smokeWindowMillis =
                Integer.valueOf(_config.getProperty(Constants.SMOKE_WINDOW_MILLIS).trim());
            etraceSampleRate =
                Integer.valueOf(_config.getProperty(Constants.ETRACE_SAMPLE_RATE).trim());
        } catch (NumberFormatException t) {
            logger.error(String.format("[%s]-- setZKProperty failed", groupName), t);
            return;
        }
        logger.info(String.format("[%s]-- setZKProperty finished", groupName));
    }

    @Override public void setZkCfg(String attr, String newValue) {
        try {
            switch (attr) {
                case Constants.FUSE_PASS_RATE:
                    fusePassRate = parseOrDefault(newValue, 4);
                    break;
                case Constants.FUSE_WINDOW_MILLIS:
                    fuseWindowMillis = parseOrDefault(newValue, 2000);
                    break;
                case Constants.SMOKE_WINDOW_MILLIS:
                    smokeWindowMillis = parseOrDefault(newValue, 2000);
                    break;
                case Constants.ETRACE_SAMPLE_RATE:
                    etraceSampleRate = parseOrDefault(newValue, 100);
                    break;
                case Constants.ALLOW_SHADOWDB:
                    allowShadowdb = "on".equalsIgnoreCase(newValue);
                    break;
                case Constants.SEND_AUDIT_SQL_TO_RMQ_THRESHOLD:
                    SendThreshold.parseThresholds(newValue);
                    break;
                case Constants.HEARTBEAT:
                    heartbeat = newValue;
                    break;
                default:
                    logger.info(String
                        .format("[%s]-- Unknown zookeeper config. node:[%s],value:[%s]", groupName,
                            attr, newValue));
                    return;
            }
        } catch (Exception t) {
            logger.error(String.format("[%s]-- setZKProperty failed", groupName), t);
            return;
        }
        logger.info(String
            .format("[%s]-- zookeeper config success. node:[%s],value:[%s]", groupName, attr,
                newValue));
    }

    public int getFusePassRate() {
        return fusePassRate;
    }

    public int getFuseWindowMillis() {
        return fuseWindowMillis;
    }

    public int getSmokeWindowMillis() {
        return smokeWindowMillis;
    }

    public int getEtraceSampleRate() {
        return etraceSampleRate;
    }

    public boolean isAllowShadowdb() {
        return allowShadowdb;
    }
}
