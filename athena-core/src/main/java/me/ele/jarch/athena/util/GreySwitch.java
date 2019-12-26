package me.ele.jarch.athena.util;

import me.ele.jarch.athena.sharding.ShardingHashCheckLevel;
import me.ele.jarch.athena.util.rmq.AuditSqlToRmq;

/**
 * Created by Dal-Dev-Team on 17/1/4.
 */
public class GreySwitch {

    private static final GreySwitch GREY_SWITCH = new GreySwitch();

    public static GreySwitch getInstance() {
        return GREY_SWITCH;
    }

    private GreySwitch() {
    }

    private volatile boolean allowGlobalIdPartition = true;

    private volatile boolean allowSendAuditSqlToRmq = false;

    private volatile long overflowTraceFrequency = -1;

    private volatile boolean smartAutoKillerOpen = false;

    private volatile long loginTimeoutInMills = 5000;

    private volatile long loginSlowInMills = 100;

    private volatile boolean allowDalGroupHealthCheck = true;

    private volatile boolean mhaSlaveFailedEnabed = true;

    private volatile int maxSqlPatternLength = 4096;

    private volatile int maxSqlPatternTruncLength = maxSqlPatternLength * 2;

    private volatile long maxSqlLength = 512_000;

    private volatile long maxResponseLength = 1024 * 1024;

    private volatile boolean isDalGroupCfgEnabled = true;

    private volatile boolean oldConfigFileDisabled = false;

    private volatile boolean zkDalgroupWatcherEnabled = true;

    private volatile boolean isQuerySqlLogEnabled = false;

    private volatile int healthCheckInterval = 120 * 1000;

    private volatile int warmUpConnectionCount = -1;

    private volatile ShardingHashCheckLevel shardingHashCheckLevel = ShardingHashCheckLevel.WARN;

    private volatile boolean isSpecifyDb = true;

    private volatile boolean dangerSqlFilterEnabled = false;

    public boolean isAllowGlobalIdPartition() {
        return allowGlobalIdPartition;
    }

    public void setAllowGlobalIdPartition(boolean allowGlobalIdPartition) {
        this.allowGlobalIdPartition = allowGlobalIdPartition;
    }

    public boolean isAllowSendAuditSqlToRmq() {
        return allowSendAuditSqlToRmq;
    }

    public void setAllowSendAuditSqlToRmq(boolean allowSendAuditSqlToRmq) {
        this.allowSendAuditSqlToRmq = allowSendAuditSqlToRmq;
        if (!this.allowSendAuditSqlToRmq) {
            AuditSqlToRmq.getInstance().reset();
        }
    }

    public void setOverflowTraceFrequency(long frequency) {
        this.overflowTraceFrequency = frequency;
    }

    public long getOverflowTraceFrequency() {
        return this.overflowTraceFrequency;
    }

    public boolean isSmartAutoKillerOpen() {
        return smartAutoKillerOpen;
    }

    public void setSmartAutoKillerOpen(boolean smartAutoKillerOpen) {
        this.smartAutoKillerOpen = smartAutoKillerOpen;
    }

    public long getLoginTimeoutInMills() {
        return loginTimeoutInMills;
    }

    public void setLoginTimeoutInMills(long loginTimeoutInMills) {
        this.loginTimeoutInMills = loginTimeoutInMills;
    }

    public long getLoginSlowInMills() {
        return loginSlowInMills;
    }

    public void setLoginSlowInMills(long loginSlowInMills) {
        this.loginSlowInMills = loginSlowInMills;
    }

    public void setAllowDalGroupHealthCheck(Boolean allowDalGroupHealthCheck) {
        this.allowDalGroupHealthCheck = allowDalGroupHealthCheck;
    }

    public boolean isAllowDalGroupHealthCheck() {
        return this.allowDalGroupHealthCheck;
    }

    public int getMaxSqlPatternLength() {
        return maxSqlPatternLength;
    }

    public void setMaxSqlPatternLength(int maxSqlPatternLength) {
        this.maxSqlPatternLength = maxSqlPatternLength;
        this.maxSqlPatternTruncLength = maxSqlPatternLength * 2;
    }

    public int getMaxSqlPatternTruncLength() {
        return maxSqlPatternTruncLength;
    }

    public boolean isMhaSlaveFailedEnabed() {
        return mhaSlaveFailedEnabed;
    }

    public void setMhaSlaveFailedEnabed(boolean mhaSlaveFailedEnabed) {
        this.mhaSlaveFailedEnabed = mhaSlaveFailedEnabed;
    }

    public boolean isDalGroupCfgEnabled() {
        return isDalGroupCfgEnabled;
    }

    public void setDalGroupCfgEnabled(boolean dalGroupCfgEnabled) {
        this.isDalGroupCfgEnabled = dalGroupCfgEnabled;
    }


    public boolean isOldConfigFileDisabled() {
        return oldConfigFileDisabled;
    }

    public void setOldConfigFileDisabled(boolean oldConfigFileDisabled) {
        this.oldConfigFileDisabled = oldConfigFileDisabled;
    }

    public boolean zkDalgroupWatcherEnabled() {
        return zkDalgroupWatcherEnabled;
    }

    public void setZkDalgroupWatcherEnabled(boolean zkDalgroupWatcherEnabled) {
        this.zkDalgroupWatcherEnabled = zkDalgroupWatcherEnabled;
    }

    public long getMaxSqlLength() {
        return maxSqlLength;
    }

    public void setMaxSqlLength(long maxSqlLength) {
        this.maxSqlLength = maxSqlLength;
    }

    public long getMaxResponseLength() {
        return maxResponseLength;
    }

    public void setMaxResponseLength(long maxResponseLength) {
        this.maxResponseLength = maxResponseLength;
    }

    public boolean isQuerySqlLogEnabled() {
        return isQuerySqlLogEnabled;
    }

    public void setQuerySqlLogEnabled(boolean querySqlLogEnabled) {
        isQuerySqlLogEnabled = querySqlLogEnabled;
    }

    public int getHealthCheckInterval() {
        return healthCheckInterval;
    }

    public void setHealthCheckInterval(int healthCheckInterval) {
        this.healthCheckInterval = healthCheckInterval;
    }

    public int getWarmUpConnectionCount() {
        return warmUpConnectionCount;
    }

    public void setWarmUpConnectionCount(int warmUpConnectionCount) {
        this.warmUpConnectionCount = warmUpConnectionCount;
    }

    public ShardingHashCheckLevel getShardingHashCheckLevel() {
        return shardingHashCheckLevel;
    }

    public void setShardingHashCheckLevel(String level) {
        this.shardingHashCheckLevel = ShardingHashCheckLevel.getCheckLevel(level);
    }

    public boolean isSpecifyDb() {
        return isSpecifyDb;
    }

    public void setSpecifyDb(boolean specifyDb) {
        isSpecifyDb = specifyDb;
    }

    public boolean isDangerSqlFilterEnabled() {
        return dangerSqlFilterEnabled;
    }

    public void setDangerSqlFilterEnabled(boolean dangerSqlFilterEnabled) {
        this.dangerSqlFilterEnabled = dangerSqlFilterEnabled;
    }

}
