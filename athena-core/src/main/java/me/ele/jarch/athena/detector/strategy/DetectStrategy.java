package me.ele.jarch.athena.detector.strategy;

import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.constant.EventTypes;
import me.ele.jarch.athena.detector.DalGroupSample;
import me.ele.jarch.athena.detector.SQLSample;
import me.ele.jarch.athena.util.etrace.TraceEnhancer;
import me.ele.jarch.athena.util.rmq.SlowSqlInfo;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by Dal-Dev-Team on 17/3/6.
 */
public abstract class DetectStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(DetectStrategy.class);

    public static final Comparator<DetectStrategy> COMPARATOR = new Comparator<DetectStrategy>() {
        @Override public int compare(DetectStrategy o1, DetectStrategy o2) {
            return Double.compare(o1.totalScore, o2.totalScore);
        }
    };

    // 该值表示上次采样和当前的所有统计值的变化率
    // 如果业务没有任何抖动,那么该值为1
    // 如果该值大于一定阈值,那么表示DB有抖动,需要各项指标检查
    protected double sqlScore = 0;
    protected double queueScore = 1;
    public double totalScore = 0;

    protected Map<String, Object> tags = new LinkedHashMap<>();

    protected String scoreInfo() {
        return String.format("[%s,sqlScore:%.2f,queueScore:%.1f] ", name(), sqlScore, queueScore);
    }

    SQLSample curSql;
    SQLSample prevSql;
    DalGroupSample curGroup;
    DalGroupSample prevGroup;
    public SlowSqlInfo slowSqlInfo;

    public final void init(SQLSample curSql, SQLSample prevSql, DalGroupSample curGroup,
        DalGroupSample prevGroup) {
        this.curSql = curSql;
        this.prevSql = prevSql;
        this.curGroup = curGroup;
        this.prevGroup = prevGroup;
        sqlScore = 0;
        queueScore = 1;
        totalScore = 0;
    }

    protected final void etrace(String pattern) {
        if (StringUtils.isEmpty(pattern)) {
            return;
        }
        slowSqlInfo = new SlowSqlInfo();
        slowSqlInfo.dalGroup = this.curGroup.dalGroup;
        slowSqlInfo.clientAppId = Constants.APPID;
        slowSqlInfo.reason = name();
        // 用于方便检索和查看, 所以把dalgroup当做是sqlId,真正的sqlId是slow_sql_id字段
        String sqlId = DigestUtils.md5Hex(pattern);
        slowSqlInfo.sqlId = sqlId;
        slowSqlInfo.sqlPattern = pattern;
        String score = String.valueOf(f3(totalScore));
        slowSqlInfo.score = score;
        Map<String, String> slowTags = new HashMap<>();
        tags.forEach((k, v) -> slowTags.put(k, Objects.toString(v)));
        slowSqlInfo.tags = slowTags;
        TraceEnhancer.newFluentEvent(EventTypes.RISK_SQL_SLOW, this.curGroup.dalGroup)
            .tag("reason", name()).tag("sqlId", sqlId)
            .data("score:" + score + " detail:" + slowTags.toString())
            .status(io.etrace.common.Constants.SUCCESS).complete();
    }

    public final void detect() {
        detect0();
        if (sqlScore != 0) {
            collectInfo();
        }
    }

    public final void recordEtrace() {
        if (totalScore == 0) {
            return;
        }
        recordEtrace0();
    }

    public final void log() {
        if (totalScore == 0) {
            return;
        }
        log0();
        LOGGER.warn(scoreInfo() + "{},{}", curSql.sqlid, tags);
    }

    // 默认实现为分数>=6,子类可修改
    public boolean isPossibleSlow() {
        return totalScore >= 6;
    }

    public boolean isSeriousSlow() {
        return totalScore >= 10;
    }

    protected abstract void detect0();

    protected abstract void recordEtrace0();

    protected abstract void collectInfo();

    protected abstract void log0();

    /**
     * 每种策略的名称
     */
    public abstract String name();

    // 根据一些属性调整分数
    public void adjustScore() {
        adjustByQueueSize();
        totalScore = queueScore * sqlScore;
    }

    //根据队列拥堵程度来调整分数
    protected void adjustByQueueSize() {
        queueScore = Math.sqrt(Math.max(1, curGroup.result.getMaxQueueSize()));
        tags.put("queueSize", String.valueOf(curGroup.result.getMaxQueueSize()));
    }

    protected static String f3(double d) {
        return String.format("%.2f", d);
    }
}
