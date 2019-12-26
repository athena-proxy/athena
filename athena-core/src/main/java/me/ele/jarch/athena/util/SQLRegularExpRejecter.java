package me.ele.jarch.athena.util;

import me.ele.jarch.athena.constant.Constants;

import java.util.regex.Pattern;

public class SQLRegularExpRejecter extends SQLRejecter {

    public SQLRegularExpRejecter(String groupName) {
        super(groupName);
    }

    @Override protected void setPattern(String pattern) {
        // 使用去除空格和括号后的pattern,防止因为空格,tab,括号等问题造成无法匹配
        rejectPattern.add(Pattern.compile(
            pattern.replaceAll("\\s+", "").replaceAll("\\(", "\\\\(").replaceAll("\\)", "\\\\)"),
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE));
    }

    @Override protected String name() {
        return Constants.REJECT_SQL_BY_REGULAR_EXP;
    }

    // 也就是说,当zookeeper设置值为select x, y ,z from eleme_order的字符串时
    // 程序内部的Pattern对象其实是selectx,y,z
    // 当有sql进入尝试match时,sql字符串优先会删除sql里所有的空格或tab,再进行match

    @Override protected boolean matchSQL(Pattern p, String originSQL, String sqlPattern) {
        // 去除空格再进行match,防止因为空格,tab等问题造成无法匹配
        return p.matcher(originSQL.replaceAll("\\s+", "")).find();
    }
}
