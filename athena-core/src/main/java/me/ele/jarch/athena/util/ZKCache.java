package me.ele.jarch.athena.util;

import com.alibaba.druid.sql.ast.SQLHint;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlSelectParser;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.constant.Metrics;
import me.ele.jarch.athena.scheduler.DBChannelDispatcher;
import me.ele.jarch.athena.sharding.sql.ShardingSQL;
import me.ele.jarch.athena.util.etrace.TraceEnhancer;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ZKCache {
    private static final Logger logger = LoggerFactory.getLogger(ZKCache.class);
    private volatile int maxResultBufSize = 10485760;
    private volatile double maxActiveDbSessions = 8.0;
    private volatile double maxActiveDBTrx = 5000.0;
    private volatile int maxQueueSize = 5000;
    private volatile String rejectSQLByPattern = "";
    private volatile String rejectSQLByRegularExpPattern = "";
    private volatile String rejectSQLByRealRegularPattern = "";
    private final SQLPatternRejecter sqlPatternRejecter;
    private final SQLRegularExpRejecter sqlRegularExpReject;
    private final SQLRealRegularRejecter sqlRealRegularRejecter;

    // for autokiller
    private volatile boolean autoKillerOpen = false;
    private volatile int autoKillerLowerSize = 8;
    private volatile int autoKillerUpperSize = 320;
    private volatile int autoKillerSlowSQL = 100;
    private volatile int autoKillerSlowCommit = autoKillerSlowSQL * 100;

    protected final String groupName;
    private String slaveOfflineFlags = "";
    private String fuseDBFlags = "";

    private volatile boolean weakMasterSwitch = false;
    /**
     * 引入`纯slave库`的概念, 所谓纯slave库是指在数据库关系拓扑中
     * 只提供读服务的实例(实现上指DB的read_only=true), 一旦该实例
     * 被提升为master库,又提供了写服务将不能称为`纯slave`库。
     * 应用场景: 大数据抽数为了避免影响线上,抽数的时候一般只会抽取
     * `纯slave库`,在XG,WG机房用作大数据抽数的slave永远不会被提升
     * 为master库。但是在ZB机房, 用作大数据抽数的slave无法完全避免
     * 被提升为master库,如果大数据在一个提升为master的原slave库上
     * 抽数，很可能会造成事故。为了解决此问题,在大数据抽数的DALGroup
     * 配置`纯slave库`偏好,保证大数据抽数不会影响线上。
     * <p>
     * `pureSlaveOnly` 为true时, 表明要求关联的DALGroup所用的slave
     * 实例一定是纯slave库
     */
    private volatile boolean pureSlaveOnly = false;

    private Set<String> offlineDbs = new TreeSet<>();

    private volatile long bindMastePeriodend = 0;
    private volatile long heartbeatFastfailPeriodEnd = 0;

    private volatile Predicate<String> whiteFieldsFilter = ShardingSQL.BLACK_FIELDS_FILTER;
    private volatile long whiteFieldsDeadline = 0;

    // When sql is rejected, delay and return error, time unit: ms.
    private volatile int delayReturnError = 100;

    private volatile Map<String, Double> trafficPolicings = new HashMap<>();
    private volatile Set<String> bindMasterSQLs = Collections.emptySet();
    private volatile Map<String, ZKSQLHint> appendIndexHints = Collections.emptyMap();

    public ZKCache(String groupName) {
        this.groupName = Objects.requireNonNull(groupName, "groupName must be not null");
        sqlPatternRejecter = new SQLPatternRejecter(groupName);
        sqlRegularExpReject = new SQLRegularExpRejecter(groupName);
        sqlRealRegularRejecter = new SQLRealRegularRejecter(groupName);
    }

    public SQLPatternRejecter getSQLByPatternRejecter() {
        return sqlPatternRejecter;
    }

    public SQLRegularExpRejecter getSQLByRegularExpRejecter() {
        return sqlRegularExpReject;
    }

    public SQLRealRegularRejecter getSQLRealRegularRejecter() {
        return sqlRealRegularRejecter;
    }

    public void init(Properties config) {
        setZKProperty(config);
    }

    public void setZkCfg(String attr, String newValue) {
        try {
            switch (attr) {
                case Constants.MAX_ACTIVE_DB_SESSIONS:
                    maxActiveDbSessions = parseOrDefault(newValue, 8.0);
                    refreshRuntime(attr);
                    break;
                case Constants.MAX_ACTIVE_DB_TRANS:
                    maxActiveDBTrx = parseOrDefault(newValue, 5000.0);
                    refreshRuntime(attr);
                    break;
                case Constants.MAX_RESULT_BUF_SIZE:
                    maxResultBufSize = parseOrDefault(newValue, 10485760);
                    break;
                case Constants.MAX_QUEUE_SIZE:
                    maxQueueSize = parseOrDefault(newValue, 5000);
                    break;
                case Constants.REJECT_SQL_BY_PATTERN:
                    rejectSQLByPattern = newValue;
                    refreshRuntime(attr);
                    break;
                case Constants.REJECT_SQL_BY_REGULAR_EXP:
                    rejectSQLByRegularExpPattern = newValue;
                    refreshRuntime(attr);
                    break;
                case Constants.REJECT_SQL_BY_REAL_REGULAR_EXP:
                    rejectSQLByRealRegularPattern = newValue;
                    refreshRuntime(attr);
                    break;
                case Constants.AK_SLOW_SQL:
                    autoKillerSlowSQL = parseOrDefault(newValue, 100);
                    break;
                case Constants.AK_SLOW_COMMIT:
                    autoKillerSlowCommit = parseOrDefault(newValue, 10000);
                    break;
                case Constants.AK_OPEN:
                    autoKillerOpen = "on".equalsIgnoreCase(newValue);
                    break;
                case Constants.AK_LOWER_SIZE:
                    autoKillerLowerSize = parseOrDefault(newValue, 8);
                    break;
                case Constants.AK_UPPER_SIZE:
                    autoKillerUpperSize = parseOrDefault(newValue, 320);
                    break;
                case Constants.DELAY_RETURN_ERROR:
                    delayReturnError = parseOrDefault(newValue, 100);
                    break;
                case Constants.SLAVE_OFFLINE_FLAGS:
                    offlineChange(newValue);
                    break;
                case Constants.FUSE_DB_FLAGS:
                    fuseChange(newValue);
                    break;
                case Constants.ELEME_ORDER_DOUBLE_WRITE:
                    grayChange(newValue);
                    break;
                case Constants.BIND_MASTER_PERIOD:
                    bindMasterPeriodChange(newValue);
                    break;
                case Constants.MASTER_HEARTBEAT_FASTFAIL_PERIOD:
                    setMasterHeartBeatFastfail(newValue);
                    break;
                case Constants.WEAK_MASTER_SWITCH:
                    weakMasterChange(newValue);
                    break;
                case Constants.WHITE_FIELDS:
                    whiteFieldsFilter = buildWhiteFieldsFilterFromString(newValue);
                    break;
                case Constants.WHITE_FIELDS_DEADLINE:
                    whiteFieldsDeadlineChange(newValue);
                    break;
                case Constants.TRAFFIC_POLICINGS:
                    trafficPolicingChange(newValue);
                    break;
                case Constants.BIND_MASTER_SQLS:
                    bindMasterSQLs = buildBindMasterSQLsFromString(newValue);
                    break;
                case Constants.APPEND_INDEX_HINTS:
                    appendIndexHints = buildAppendIndexHintsFromString(newValue);
                    break;
                case Constants.PURE_SLAVE_ONLY:
                    pureSlaveOnly = "on".equalsIgnoreCase(newValue);
                    break;
                default:
                    logger.warn("[{}]-- ignore unknown zookeeper config. node:[{}],value:[{}]",
                        groupName, attr, newValue);
                    return;
            }
        } catch (Exception t) {
            logger.error("[{}]-- setZKProperty failed", groupName, t);
            return;
        }
        TraceEnhancer.newFluentEvent(Metrics.CONFIG_ZK_CHANGED, groupName)
            .data("attribute: " + attr + ", newValue: " + newValue)
            .status(io.etrace.common.Constants.SUCCESS).complete();
        logger.info("[{}]-- zookeeper config success. node:[{}],value:[{}]", groupName, attr,
            newValue);
    }

    private void bindMasterPeriodChange(String newValue) {
        SimpleDateFormat parserSDF = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        if (newValue.trim().isEmpty()) {
            this.bindMastePeriodend = 0;
            return;
        }
        try {
            long bindMastePeriodEnd = parserSDF.parse(newValue.trim()).getTime();
            this.bindMastePeriodend = bindMastePeriodEnd;
        } catch (Exception e) {
            this.bindMastePeriodend = 0;
            logger.error(String.format("[%s]-- failed to parse bindMasterPeriod", groupName), e);
        }
    }

    private void setMasterHeartBeatFastfail(String newValue) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        /*
         * There are three conditions here: 1. Value is set to "" : Than we set heartbeatFastfailPeriodEnd to 0 2. Value is parsed correctly: Change
         * heartbeatFastfailPeriodEnd to new value 3. Failed to parse value: set to 0.
         */
        if (newValue.trim().isEmpty()) {
            heartbeatFastfailPeriodEnd = 0;
            FastFailFilter.update();
            return;
        }
        try {
            heartbeatFastfailPeriodEnd = format.parse(newValue.trim()).getTime();
            FastFailFilter.update();
        } catch (Exception e) {
            heartbeatFastfailPeriodEnd = 0;
            logger.error(
                String.format("[%s]-- failed to parse masterHeartbeatFastfailPeriod", groupName),
                e);
        }
    }

    public void setZKProperty(Properties _config) {
        try {
            maxResultBufSize =
                Integer.valueOf(_config.getProperty(Constants.MAX_RESULT_BUF_SIZE).trim());
            maxActiveDbSessions =
                Integer.valueOf(_config.getProperty(Constants.MAX_ACTIVE_DB_SESSIONS).trim());
            maxActiveDBTrx =
                Integer.valueOf(_config.getProperty(Constants.MAX_ACTIVE_DB_TRANS).trim());
            maxQueueSize = Integer.valueOf(_config.getProperty(Constants.MAX_QUEUE_SIZE).trim());
            rejectSQLByPattern = _config.getProperty(Constants.REJECT_SQL_BY_PATTERN);
            rejectSQLByRegularExpPattern = _config.getProperty(Constants.REJECT_SQL_BY_REGULAR_EXP);
        } catch (Exception t) {
            logger.error(String.format("[%s]-- setZKProperty failed", groupName), t);
            return;
        }
        logger.info(String.format("[%s]-- setZKProperty finished", groupName));
    }

    private void refreshRuntime(String attr) {
        if (attr.equals(Constants.MAX_ACTIVE_DB_SESSIONS)) {
            DBChannelDispatcher.getHolders().get(groupName).getScheds()
                .forEach(scheduler -> scheduler.setMaxActiveDBSessions(maxActiveDbSessions));
        } else if (attr.equals(Constants.MAX_ACTIVE_DB_TRANS)) {
            DBChannelDispatcher.getHolders().get(groupName).getScheds()
                .forEach(scheduler -> scheduler.setMaxActiveDBTrx(maxActiveDBTrx));
        } else if (attr.equals(Constants.REJECT_SQL_BY_PATTERN)) {
            sqlPatternRejecter.setRejectPattern(rejectSQLByPattern);
        } else if (attr.equals(Constants.REJECT_SQL_BY_REGULAR_EXP)) {
            sqlRegularExpReject.setRejectPattern(rejectSQLByRegularExpPattern);
        } else if (attr.equals(Constants.REJECT_SQL_BY_REAL_REGULAR_EXP)) {
            sqlRealRegularRejecter.setRejectPattern(rejectSQLByRealRegularPattern);
        }
    }

    public int getMaxResultBufSize() {
        return maxResultBufSize;
    }

    public double getMaxActiveDbSessions() {
        return maxActiveDbSessions;
    }

    public int getMaxQueueSize() {
        return maxQueueSize;
    }

    public String getRejectSQLByPattern() {
        return rejectSQLByPattern;
    }

    public String getRejectSQLByRegularExpPattern() {
        return rejectSQLByRegularExpPattern;
    }

    public int getLowerAutoKillerSize() {
        return autoKillerLowerSize;
    }

    public int getSmartLowerAutoKillerSize() {
        if (GreySwitch.getInstance().isSmartAutoKillerOpen()) {
            return 0;
        }
        return autoKillerLowerSize;
    }

    public int getUpperAutoKillerSize() {
        return autoKillerUpperSize;
    }

    public int getSmartUpperAutoKillerSize() {
        if (GreySwitch.getInstance().isSmartAutoKillerOpen()) {
            return 0;
        }
        return autoKillerUpperSize;
    }

    public boolean isAutoKillerOpen() {
        return autoKillerOpen;
    }

    public int getAutoKillerSlowSQL() {
        return autoKillerSlowSQL;
    }

    public int getSmartAutoKillerSlowSQL(int queueSize) {
        if (!GreySwitch.getInstance().isSmartAutoKillerOpen()) {
            return autoKillerSlowSQL;
        }
        int upper = autoKillerUpperSize;
        int blockedSize = queueSize;
        int slowTime = this.autoKillerSlowSQL;
        int smartSlowTime = Integer.MAX_VALUE;
        while (upper > 0) {
            if (blockedSize >= upper) {
                smartSlowTime = slowTime;
                break;
            }
            upper = upper >> 1;
            slowTime = slowTime << 1;
        }
        return smartSlowTime;
    }

    public int getAutoKillerSlowCommit() {
        return autoKillerSlowCommit;
    }

    public int getDelayReturnError() {
        return delayReturnError;
    }

    public long getHeartbeatFastfailPeriodEnd() {
        return heartbeatFastfailPeriodEnd;
    }

    private Map<String, ZKSQLHint> buildAppendIndexHintsFromString(String value) {
        if (StringUtils.isEmpty(value)) {
            return Collections.emptyMap();
        }
        Map<String, ZKSQLHint> sqlHashWithHints = new HashMap<>();
        for (String block : value.split(";")) {
            String[] splitedBlock = block.split(":");
            if (splitedBlock.length != 2) {
                continue;
            }
            String sqlHash = splitedBlock[0].trim();
            List<SQLHint> hints = Collections.emptyList();
            try {
                hints = buildSQLHintsFromString(splitedBlock[1].trim());
            } catch (Exception e) {
                logger.warn("zk configed index hint syntax error, ignored", e);
            }
            if (hints.isEmpty()) {
                continue;
            }
            sqlHashWithHints.put(sqlHash, new ZKSQLHint(hints, splitedBlock[1]));
        }
        return sqlHashWithHints;
    }

    private List<SQLHint> buildSQLHintsFromString(String indexHints) {
        String partSQL = String.format("guide_table_for_build_hints_node %s", indexHints);
        MySqlSelectParser parser = new MySqlSelectParser(partSQL);
        return parser.parseTableSource().getHints();
    }

    private Set<String> buildDBStatusByString(String sbStatusString) {
        Set<String> newOfflineDBs = new TreeSet<>();
        String[] confList = sbStatusString.trim().split(";");
        for (String qualifiedId : confList) {
            newOfflineDBs.add(qualifiedId.trim());
        }
        return newOfflineDBs;
    }

    private static Set<String> buildFuseDBsByString(String fuseString) {
        Set<String> fuseDBs = new HashSet<>();
        String[] confList = fuseString.trim().split(";");
        for (String s : confList) {
            if (s.contains(":")) {
                fuseDBs.add(s.trim());
            }
        }
        return fuseDBs;
    }

    private static Set<String> buildBindMasterSQLsFromString(String value) {
        if (StringUtils.isEmpty(value)) {
            return Collections.emptySet();
        }
        return Stream.of(value.split(";")).map(s -> s.trim()).filter(StringUtils::isNotEmpty)
            .collect(Collectors.toSet());
    }

    private static void grayChange(String newGrayState) {
        //NOOP
    }

    protected static int parseOrDefault(String parseStr, int defaultValue) {
        return parseStr.isEmpty() ? defaultValue : Integer.parseInt(parseStr);
    }

    protected static double parseOrDefault(String parseStr, double defaultValue) {
        return parseStr.isEmpty() ? defaultValue : Double.parseDouble(parseStr);
    }

    private void offlineChange(String newValue) {
        if (slaveOfflineFlags.equals(newValue))
            return;
        this.setOfflineDbs(buildDBStatusByString(newValue));
        DBChannelDispatcher dispatcher = DBChannelDispatcher.getHolders().get(this.groupName);
        dispatcher.updateDbInfo(dispatcher.getDalGroupConfig().getDbInfos());
        slaveOfflineFlags = newValue;
    }

    private void fuseChange(String newValue) {
        if (fuseDBFlags.equals(newValue))
            return;
        Set<String> fuseDBs = buildFuseDBsByString(newValue);
        DBChannelDispatcher.getHolders().get(groupName).getSickMonitor().sick(fuseDBs);
        fuseDBFlags = newValue;
    }

    private void weakMasterChange(String newValue) {
        weakMasterSwitch = "on".equalsIgnoreCase(newValue);
        DBChannelDispatcher dispatcher = DBChannelDispatcher.getHolders().get(groupName);
        WeakMasterFilter.update(dispatcher);
    }

    private Predicate<String> buildWhiteFieldsFilterFromString(String value) {
        if (StringUtils.isEmpty(value)) {
            return ShardingSQL.BLACK_FIELDS_FILTER;
        }
        Set<String> result = new TreeSet<>();
        for (String field : value.split(",")) {
            String trimedField = field.trim();
            if (StringUtils.isNotEmpty(trimedField)) {
                result.add(trimedField);
            }
        }
        return result::contains;
    }

    private void whiteFieldsDeadlineChange(String newValue) {
        if (StringUtils.isEmpty(newValue)) {
            whiteFieldsDeadline = 0;
        }
        SimpleDateFormat parserSDF = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        try {
            whiteFieldsDeadline = parserSDF.parse(newValue.trim()).getTime();
        } catch (Exception e) {
            whiteFieldsDeadline = 0;
            logger.error(String.format("[%s]-- failed to parse whiteFieldsDeadline", groupName), e);
        }
    }

    private void trafficPolicingChange(String newValue) {
        final Map<String, Double> tp = new HashMap<>();
        for (String line : newValue.split(";")) {
            if (line.split(":").length != 2) {
                continue;
            }
            String sqlid = line.split(":")[0].trim();
            Double rate = Double.valueOf(line.split(":")[1].trim());
            tp.put(sqlid, rate);
        }
        if (tp.keySet().contains("*")) {
            tp.keySet().removeIf(k -> !"*".equals(k));
        }
        this.trafficPolicings = tp;
        DBChannelDispatcher.getHolders().get(groupName)
            .updateSQLTrafficPolicing(this.trafficPolicings);

    }

    public double getMaxActiveDBTrx() {
        return maxActiveDBTrx;
    }

    public String getGroupName() {
        return groupName;
    }

    public Set<String> getOfflineDbs() {
        return offlineDbs;
    }

    public void setOfflineDbs(Set<String> offlineDbs) {
        this.offlineDbs = offlineDbs;
    }

    public boolean isInBindMasterPeriod() {
        return System.currentTimeMillis() <= this.bindMastePeriodend;
    }

    public boolean isInHeartbeatFastfailPeriod() {
        return System.currentTimeMillis() <= heartbeatFastfailPeriodEnd;
    }

    public boolean isInWeakMasterMode() {
        return weakMasterSwitch;
    }

    public Predicate<String> getZKWhiteFieldsFilter() {
        if (whiteFieldsDeadline == 0 || System.currentTimeMillis() > whiteFieldsDeadline) {
            return ShardingSQL.BLACK_FIELDS_FILTER;
        }
        return whiteFieldsFilter;
    }

    public Map<String, Double> getTrafficPolicings() {
        return trafficPolicings;
    }

    public boolean isBindMasterSQL(String sqlHash) {
        return bindMasterSQLs.contains(sqlHash);
    }

    public boolean isAppendIndexHintsSQL(String sqlHash) {
        return appendIndexHints.containsKey(sqlHash);
    }

    public ZKSQLHint getAppendIndexHintsBySQLHash(String sqlHash) {
        return appendIndexHints.getOrDefault(sqlHash, new ZKSQLHint());
    }

    public boolean isPureSlaveOnly() {
        return pureSlaveOnly;
    }
}
