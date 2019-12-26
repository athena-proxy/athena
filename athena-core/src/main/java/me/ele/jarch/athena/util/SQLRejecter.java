package me.ele.jarch.athena.util;

import me.ele.jarch.athena.SQLLogFilter;
import me.ele.jarch.athena.constant.Metrics;
import me.ele.jarch.athena.constant.TraceNames;
import me.ele.jarch.athena.util.etrace.MetricFactory;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

abstract class SQLRejecter {
    private static final Logger logger = LoggerFactory.getLogger(SQLRejecter.class);

    protected volatile Set<Pattern> rejectPattern;

    private final String groupName;

    protected SQLRejecter(String groupName) {
        this.groupName = groupName;
    }

    protected abstract String name();

    protected abstract void setPattern(String pattern);

    public synchronized void setRejectPattern(String pattern) {
        if (pattern == null) {
            rejectPattern = null;
            logger.info(String.format("[%s] %s is disabled", groupName, name()));
            return;
        }
        pattern = pattern.trim();
        if (pattern.isEmpty()) {
            rejectPattern = null;
            logger.info(String.format("[%s] %s is disabled", groupName, name()));
            return;
        }
        try {
            rejectPattern = new HashSet<>();
            setPattern(pattern);
        } catch (Exception e) {
            rejectPattern = null;
            logger.error(String.format("Illegal reject regex: [%s]", pattern), e);
        }

        logger.info(String.format("[%s] %s is enabled to [%s]", groupName, name(), pattern));
    }

    public boolean rejectSQL(String originSQL, String sqlPattern, String clientAddr) {
        Set<Pattern> pattern = this.rejectPattern;
        if (pattern == null) {
            return false;
        }

        if (StringUtils.isEmpty(originSQL) || StringUtils.isEmpty(sqlPattern)) {
            return false;
        }

        for (Pattern p : pattern) {
            if (matchSQL(p, originSQL, sqlPattern)) {
                SQLLogFilter.error(String
                    .format("[%s] [%s] SQL [%s] is rejected", groupName, clientAddr,
                        originSQL.replace('\n', ' ')));
                MetricFactory.newCounter(Metrics.REJECT_SQL).addTag(TraceNames.DALGROUP, groupName)
                    .once();
                return true;
            }
        }
        return false;
    }

    abstract protected boolean matchSQL(Pattern p, String originSQL, String sqlPattern);

}
