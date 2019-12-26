package me.ele.jarch.athena.util;

import me.ele.jarch.athena.constant.Constants;
import org.apache.commons.codec.digest.DigestUtils;

import java.util.Objects;
import java.util.regex.Pattern;

public class SQLPatternRejecter extends SQLRejecter {
    private static final int SQLID_LENGTH = 32;

    public SQLPatternRejecter(String groupName) {
        super(groupName);
    }

    @Override protected void setPattern(String pattern) {
        for (String p : pattern.split(";")) {
            rejectPattern.add(Pattern.compile(p, Pattern.LITERAL));
        }
    }

    @Override protected String name() {
        return Constants.REJECT_SQL_BY_PATTERN;
    }

    @Override protected boolean matchSQL(Pattern p, String originSQL, String sqlPattern) {
        // 常规根据SQL Pattern拒绝
        if (p.matcher(sqlPattern).matches()) {
            return true;
        }
        // configPattern为ZK里的配置的每一项,可能是SqlPattern或SqlId
        String configPattern = p.pattern();
        if (Objects.isNull(configPattern)) {
            return false;
        }
        configPattern = configPattern.trim();
        // 如果不是sqlId配置,那么提前返回
        if (!startWithSqlId(configPattern)) {
            return false;
        }
        String sqlId = DigestUtils.md5Hex(sqlPattern);
        // 只根据sqlId拒绝SQL
        if (configPattern.length() == SQLID_LENGTH) {
            return configPattern.equals(sqlId);
        }
        // 根据sqlId以及后续附带的信息拒绝SQL
        // 如: 4c15c759d8707a99b700666e51d0c3ef target_id  =  200
        // 则 moreRejectInfo = "target_id=200"
        String moreRejectInfo = configPattern.substring(SQLID_LENGTH).trim().replace(" ", "");
        if (moreRejectInfo.isEmpty()) {
            return false;
        }
        // DAL会比对SQL的SqlId以及后续附带的信息拒绝SQL
        // 后续信息比较的策略为去除空白符后的比较
        if (sqlId.equals(configPattern.substring(0, SQLID_LENGTH)) && originSQL
            .replaceAll("\\s+", "").contains(moreRejectInfo)) {
            return true;
        }
        return false;
    }

    /**
     * 判断该设置是否为SqlId
     */
    private static boolean startWithSqlId(String pattern) {
        if (pattern.length() < SQLID_LENGTH) {
            return false;
        }
        for (int i = 0; i < SQLID_LENGTH; i++) {
            char c = pattern.charAt(i);
            if (!(c >= 'a' && c <= 'z') && !(c >= '0' && c <= '9')) {
                return false;
            }
        }
        return true;
    }
}
