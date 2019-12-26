package me.ele.jarch.athena.detector.strategy;

import me.ele.jarch.athena.detector.DetectUtil;
import me.ele.jarch.athena.sql.QUERY_TYPE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Created by Dal-Dev-Team on 17/3/6.
 */
public class SuddenlySlow extends DetectStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(SuddenlySlow.class);

    @Override public String name() {
        return "sudden_slow";
    }

    @Override public void detect0() {
        if (Objects.isNull(prevSql) || curSql.type == QUERY_TYPE.COMMIT || Objects.isNull(curGroup)
            || Objects.isNull(prevGroup)) {
            return;
        }
        if (curSql.result.getAvgDurInAllWindow() == 0) {
            return;
        }
        double sqlDurIncRate = DetectUtil
            .div(curSql.result.getAvgDurInCurWindow(), prevSql.result.getAvgDurInCurWindow());
        if (sqlDurIncRate < 1.2) {
            return;
        }
        double groupDurRatePrev =
            curGroup.result.getAvgDurInCurWindow() / prevGroup.result.getAvgDurInCurWindow();
        if (groupDurRatePrev < 1.2) {
            return;
        }
        double groupDurRateAll =
            curGroup.result.getAvgDurInCurWindow() / curGroup.result.getAvgDurInAllWindow();
        double sqlDurRate = DetectUtil
            .div(curSql.result.getAvgDurInCurWindow(), curSql.result.getAvgDurInAllWindow());
        sqlScore = groupDurRateAll * Math.log1p(sqlDurRate);
    }

    @Override protected void recordEtrace0() {
        etrace(prevSql.getPattern());
    }

    @Override protected void log0() {
        //        LOGGER.warn("{}", Arrays.toString(curSql.getDistribute()));
    }

    @Override protected void collectInfo() {
        tags.put("curAvg/allavg/prevAvg", String
            .format("%s/%s/%s", f3(curSql.result.getAvgDurInCurWindow()),
                f3(curSql.result.getAvgDurInAllWindow()),
                f3(prevSql.result.getAvgDurInCurWindow())));

        tags.put("groupAvg/allAvg", String
            .format("%s/%s", f3(curGroup.result.getAvgDurInCurWindow()),
                f3(curGroup.result.getAvgDurInAllWindow())));
    }
}
