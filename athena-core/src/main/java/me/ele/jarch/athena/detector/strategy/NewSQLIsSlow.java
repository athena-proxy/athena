package me.ele.jarch.athena.detector.strategy;

import me.ele.jarch.athena.detector.DetectUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Created by Dal-Dev-Team on 17/3/6.
 */
public class NewSQLIsSlow extends DetectStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(NewSQLIsSlow.class);

    @Override public String name() {
        return "newsql_slow";
    }

    @Override public void detect0() {
        if (Objects.isNull(prevSql) && Objects.nonNull(prevGroup) &&// 新SQL
            prevGroup.result.getAvgDurInCurWindow() != 0) {
            double avgRate = DetectUtil.div(curGroup.result.getAvgDurInCurWindow(),
                prevGroup.result.getAvgDurInCurWindow());
            sqlScore = curSql.result.getAvgDurInCurWindow() / 1000 * avgRate / 2;
        }
    }

    @Override protected void recordEtrace0() {
        etrace(curSql.getPattern());
    }

    @Override protected void log0() {
    }

    @Override protected void collectInfo() {
        tags.put("sql_avg", f3(curSql.result.getAvgDurInCurWindow()));
        tags.put("curGroupQps/prevGroupQps",
            String.format("%s/%s", f3(curGroup.result.getQps()), f3(prevGroup.result.getQps())));
    }
}
