package me.ele.jarch.athena.util;

import com.alibaba.druid.sql.ast.SQLHint;

import java.util.Collections;
import java.util.List;

/**
 * @author Tsai
 */
public class ZKSQLHint {
    public final List<SQLHint> hints;
    public final String hintsStr;

    public ZKSQLHint() {
        this.hints = Collections.emptyList();
        this.hintsStr = "";
    }

    public ZKSQLHint(List<SQLHint> hints, String hintsStr) {
        this.hints = hints;
        this.hintsStr = hintsStr;
    }
}
