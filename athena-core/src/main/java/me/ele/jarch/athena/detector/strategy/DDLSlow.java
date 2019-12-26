package me.ele.jarch.athena.detector.strategy;

import me.ele.jarch.athena.sql.QUERY_TYPE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Dal-Dev-Team on 17/3/7.
 */
public class DDLSlow extends DetectStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(DDLSlow.class);

    @Override public void detect0() {
        if (curSql.type == QUERY_TYPE.COMMIT) {
            sqlScore = curSql.result.getAvgDurInCurWindow() / 200;
        }
    }

    @Override protected void recordEtrace0() {
        etrace(curSql.getPattern());
    }

    @Override protected void log0() {
    }

    @Override protected void collectInfo() {
        tags.put("avgDur", curSql.result.getAvgDurInCurWindow());
    }

    @Override public String name() {
        return "ddl_slow";
    }
}
