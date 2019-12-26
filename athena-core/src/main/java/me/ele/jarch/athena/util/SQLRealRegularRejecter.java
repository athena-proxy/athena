package me.ele.jarch.athena.util;

import me.ele.jarch.athena.constant.Constants;

import java.util.regex.Pattern;

public class SQLRealRegularRejecter extends SQLRejecter {

    public SQLRealRegularRejecter(String groupName) {
        super(groupName);
    }

    @Override protected void setPattern(String pattern) {
        // 不去除空格
        rejectPattern.add(Pattern
            .compile(pattern, Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE));
    }

    @Override protected String name() {
        return Constants.REJECT_SQL_BY_REAL_REGULAR_EXP;
    }

    // 不去除空格

    @Override protected boolean matchSQL(Pattern p, String noCommentsQuery, String sqlPattern) {
        // 去除空格再进行match,防止因为空格,tab等问题造成无法匹配
        return p.matcher(noCommentsQuery).find();
    }
}
