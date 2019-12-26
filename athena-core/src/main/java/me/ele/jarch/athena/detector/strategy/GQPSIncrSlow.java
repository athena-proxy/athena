package me.ele.jarch.athena.detector.strategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Created by Dal-Dev-Team on 17/3/6.
 */
public class GQPSIncrSlow extends DetectStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(GQPSIncrSlow.class);

    private double durFactor = 1.0;

    private String reason = "group_qps_increasing";

    private static final String SLOW_SQL_INCR = "slow_sql_qps_increasing";

    @Override public void detect0() {
        if (Objects.isNull(prevGroup) || prevGroup.getCount() == 0 || curGroup.getCount() == 0
            || curSql.result.getAvgDurInAllWindow() == 0) {
            return;
        }
        double sqlDurRateAll =
            curSql.result.getAvgDurInCurWindow() / curSql.result.getAvgDurInAllWindow();
        if (sqlDurRateAll < 1.5) {
            return;
        }
        double groupDurRatePrev =
            curGroup.result.getAvgDurInCurWindow() / prevGroup.result.getAvgDurInCurWindow();
        if (groupDurRatePrev < 1.5) {
            return;
        }
        // 防止出现负数
        durFactor = Math.log1p(curSql.result.getAvgDurInCurWindow());
        durFactor = durFactor * durFactor;
        double groupDurRateAll =
            curGroup.result.getAvgDurInCurWindow() / curGroup.result.getAvgDurInAllWindow();
        double qpsRate = curGroup.result.getQps() / prevGroup.result.getQps();
        sqlScore =
            groupDurRateAll * Math.log1p(sqlDurRateAll) * Math.max(qpsRate, durFactor) / 100.0;

        // 判断是因group qps上涨,还是单条慢SQL上涨所致
        if (durFactor > qpsRate) {
            reason = SLOW_SQL_INCR;
        }
    }

    // 如果SQL本身就是dur很大的,比如5s,那么翻倍到10s所造成的影响远比5ms->10ms大得多
    private double sqlRadio(double sqlDur) {
        if (sqlDur < 1000) {
            return 1.0;
        }
        return Math.min(150, sqlDur / 20);
    }

    @Override protected void recordEtrace0() {
        etrace(curGroup.dalGroup);
    }

    @Override protected void log0() {
        //        LOGGER.warn("{}", Arrays.toString(curSql.getDistribute()));
    }

    @Override protected void collectInfo() {
        tags.put("curGroupQps/prevGroupQps",
            String.format("%s/%s", f3(curGroup.result.getQps()), f3(prevGroup.result.getQps())));
        tags.put("curAvgDur/allAvgDur", String
            .format("%s/%s", f3(curSql.result.getAvgDurInCurWindow()),
                f3(curSql.result.getAvgDurInAllWindow())));
        tags.put("groupAvg/allAvg", String
            .format("%s/%s", f3(curGroup.result.getAvgDurInCurWindow()),
                f3(curGroup.result.getAvgDurInAllWindow())));
        tags.put("durFactor", f3(durFactor));
    }

    @Override public String name() {
        return reason;
    }
}
