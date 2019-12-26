package me.ele.jarch.athena.detector;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import me.ele.jarch.athena.detector.strategy.*;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.sql.CmdQuery;
import me.ele.jarch.athena.util.etrace.EtracePatternUtil;
import me.ele.jarch.athena.util.rmq.SlowSqlInfo;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Dal-Dev-Team on 17/3/1.
 */
public class SlowDetector {
    private static final Logger LOGGER = LoggerFactory.getLogger(SlowDetector.class);

    private boolean isCalced = false;

    public LocalDateTime start = LocalDateTime.now();

    public LocalDateTime endTM = start;

    public Map<String, SQLSample> sqlSamples = new ConcurrentHashMap<>();

    public Map<String, DalGroupSample> groupSamples = new ConcurrentHashMap<>();

    public final PatternExpandProhibit prohibit = new PatternExpandProhibit();

    private static final AtomicInteger round = new AtomicInteger();

    private final AtomicLong addSampleDur = new AtomicLong();

    public static final Map<String, GroupAvg> groupAvgs = new ConcurrentHashMap<>();


    // 防止SQL Pattern过多导致问题的防御
    public class PatternExpandProhibit {
        public final AtomicInteger count = new AtomicInteger();

        public void incrementAndTryProhibit() {
            if (count.getAndIncrement() > EtracePatternUtil.MAX_CACHED_PATTERNS) {
                LOGGER.error("pattern size is too large, clear it");
                sqlSamples.clear();
                sqlSamples.put("pattern size is too large", new SQLSample());
                count.set(1);
            }
        }
    }

    public void addSample(long dur, SqlSessionContext ctx) {
        long start = System.currentTimeMillis();
        CmdQuery query = ctx.curCmdQuery;
        if (Objects.isNull(query)) {
            return;
        }
        if (Objects.isNull(query.shardingSql)) {
            return;
        }
        String pattern = query.shardingSql.getOriginMostSafeSQL();
        if (StringUtils.isEmpty(pattern)) {
            return;
        }
        SQLSample sqlSample = sqlSamples.computeIfAbsent(pattern, x -> {
            prohibit.incrementAndTryProhibit();
            return new SQLSample();
        });
        sqlSample.addSample(dur, ctx);
        String dalgroup = ctx.getHolder().getDalGroup().getName();
        DalGroupSample dalGroupSample =
            groupSamples.computeIfAbsent(dalgroup, x -> new DalGroupSample());
        dalGroupSample.addSample(dur, ctx);
        addSampleDur.addAndGet(System.currentTimeMillis() - start);
    }

    void endDetect() {
        endTM = LocalDateTime.now();
        long dur = endTM.toInstant(ZoneOffset.UTC).toEpochMilli() - start.toInstant(ZoneOffset.UTC)
            .toEpochMilli();
        sqlSamples.values().forEach(x -> x.setCurWindowDur(dur));
        groupSamples.values().forEach(x -> {
            x.calcQueueSize();
            x.setCurWindowDur(dur);
        });
    }

    protected void calc() {
        if (isCalced) {
            return;
        }
        sqlSamples.forEach((k, v) -> v.calc());
        groupSamples.forEach((k, v) -> {
            v.calc();
            // 通过(old_avg*old_count+this_dur_sum)/(old_count+this_count)
            // 更新全局的dalgroup平均响应时间
            GroupAvg groupAvg = groupAvgs.computeIfAbsent(v.dalGroup, y -> new GroupAvg());
            groupAvg.putData(v.getDurSumInCurWindow(), v.getCount());
            v.result.setAvgDurInAllWindow(groupAvg.getAvgDur());
        });
        isCalced = true;
    }

    // 把that里有但this里没有的SQLSample加入进去
    protected void mergeSQLSampleIfAbsent(SlowDetector prev) {
        prev.sqlSamples.forEach((pattern, sample) -> {
            this.sqlSamples.computeIfAbsent(pattern, x -> {
                prohibit.incrementAndTryProhibit();
                return sample;
            });
        });
    }

    private static DetectStrategy[] newStrategys() {
        return new DetectStrategy[] {
            // @formatter:off
                // 个别条件的某种SQL突然变慢,如where里的某个参数改变等
                new SuddenlySlow(),
                // 突然有一条新SQL导致变慢
                new NewSQLIsSlow(),
                // 整个Group的QPS提高而导致的变慢
                new GQPSIncrSlow(),
                // DDL导致变慢
                new DDLSlow(),
                // @formatter:on
        };
    }

    private Map<String, Map<String, List<DetectStrategy>>> computePossibleStrategies(
        SlowDetector prev) {
        Map<String, Map<String, List<DetectStrategy>>> groupWithPossibleStrategies =
            new TreeMap<>();
        sqlSamples.forEach((pattern, curSql) -> {
            SQLSample prevSql = prev.sqlSamples.get(pattern);
            if (StringUtils.isEmpty(curSql.dalGroup)) {
                return;
            }
            DalGroupSample curGroup = groupSamples.get(curSql.dalGroup);
            if (Objects.isNull(curGroup)) {
                return;
            }
            // 队列拥堵程度
            if (curGroup.result.getMaxQueueSize() <= 1) {
                return;
            }
            // 比较group整体平均avg
            if (curGroup.result.getAvgDurInCurWindow()
                <= curGroup.result.getAvgDurInAllWindow() * 1.5) {
                return;
            }
            // 该条件可解决某些慢SQL一旦执行(周期性),整体group被拖慢,但属于正常现象的情形
            // 比较单条SQL的平均avg
            // 400是冒烟查杀的时间*2, 即那些小于200ms且增量小于1.5倍的,不告警
            if (curSql.result.getAvgDurInAllWindow() < 200 && curSql.result.getAvgDurInCurWindow()
                <= curSql.result.getAvgDurInAllWindow() * 1.5) {
                return;
            }
            if (curSql.result.getAvgDurInCurWindow() <= curSql.result.getAvgDurInAllWindow()) {
                return;
            }
            // maybe null
            DalGroupSample prevGroup = prev.groupSamples.get(curSql.dalGroup);

            DetectStrategy[] sqlStrategies = newStrategys();

            for (DetectStrategy detectStrategy : sqlStrategies) {
                detectStrategy.init(curSql, prevSql, curGroup, prevGroup);
                detectStrategy.detect();
                detectStrategy.adjustScore();
                // 只打印有可能slow的SQL
                if (detectStrategy.isPossibleSlow()) {
                    groupWithPossibleStrategies
                        .computeIfAbsent(curGroup.dalGroup, x -> Maps.newTreeMap())
                        .computeIfAbsent(curSql.getPattern(), x -> Lists.newLinkedList())
                        .add(detectStrategy);
                }
            }
        });
        return groupWithPossibleStrategies;
    }

    private List<DetectStrategy> computeSeriousByPossibleStrategies(String group,
        Map<String, List<DetectStrategy>> sqlStrategies) {
        List<DetectStrategy> serious = new ArrayList<>();
        LOGGER.info("Detecting group:[{}]", group);
        LOGGER.info("Possible:");
        sqlStrategies.forEach((pattern, possibleStrategies) -> {
            if (possibleStrategies.isEmpty()) {
                return;
            }
            possibleStrategies.forEach(s -> s.log());
            DetectStrategy max = possibleStrategies.stream().max(DetectStrategy.COMPARATOR).get();
            if (max.isSeriousSlow()) {
                serious.add(max);
            }
        });
        if (!serious.isEmpty()) {
            LOGGER.info("Serious:");
            serious.stream().forEach(s -> s.log());
        }
        return serious;
    }

    private boolean computeGroupQpsIncrease(String group,
        Map<String, List<DetectStrategy>> sqlStrategies, List<DetectStrategy> serious) {
        // 该group超过1/2的SQL都报错group increase或者SuddenlySlow
        long slowCount = serious.stream().filter(
            strategy -> strategy instanceof GQPSIncrSlow || strategy instanceof SuddenlySlow)
            .count();
        if (sqlStrategies.size() >= 8 && slowCount >= sqlStrategies.size() / 2) {
            LOGGER.info("MAX:");
            LOGGER.info("Group:[{}] QPS increased", group);
            return true;
        }
        return false;
    }

    private void computeMaxStrategy(List<DetectStrategy> serious,
        LinkedBlockingQueue<SlowSqlInfo> infos) {
        DetectStrategy max = serious.stream().max(DetectStrategy.COMPARATOR).get();
        LOGGER.info("MAX:");
        max.recordEtrace();
        if (Objects.nonNull(max.slowSqlInfo)) {
            infos.offer(max.slowSqlInfo);
            max.slowSqlInfo = null;
        }
        max.log();
    }

    void detectWithPrev(SlowDetector prev, LinkedBlockingQueue<SlowSqlInfo> infos) {
        long start = System.currentTimeMillis();
        this.calc();
        prev.calc();
        // 初始N轮不打印输出
        if (round.incrementAndGet() < 3) {
            LOGGER.info("This round is only collecting information");
            return;
        }
        // group -> {sqlPattern -> [Strategy,...]}
        Map<String, Map<String, List<DetectStrategy>>> groupWithPossibleStrategies =
            computePossibleStrategies(prev);
        if (groupWithPossibleStrategies.isEmpty()) {
            return;
        }
        LOGGER.info("======================== Detect ================================");
        groupWithPossibleStrategies.forEach((group, sqlStrategies) -> {
            if (sqlStrategies.isEmpty()) {
                return;
            }
            List<DetectStrategy> serious = computeSeriousByPossibleStrategies(group, sqlStrategies);
            if (serious.isEmpty()) {
                return;
            }
            DalGroupSample groupSample = groupSamples.get(group);
            if (Objects.isNull(groupSample)) {
                return;
            }
            if (computeGroupQpsIncrease(group, sqlStrategies, serious)) {
                return;
            }
            computeMaxStrategy(serious, infos);
        });
        LOGGER.info("======================== Detect END ========= calc/addSample:{}ms/{}ms",
            System.currentTimeMillis() - start, addSampleDur.get());
    }
}
