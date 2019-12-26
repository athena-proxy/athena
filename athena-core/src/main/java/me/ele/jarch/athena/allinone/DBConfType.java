package me.ele.jarch.athena.allinone;

import org.apache.commons.lang3.StringUtils;

/**
 * @author shaoyang.qi
 * <p>
 * # indicates comments
 * <p>
 * $ indicates special uses for some internal module
 * <p>
 * \@ indicates delay connect string for DelayRetrieve
 */
public enum DBConfType {
    NONE("#"), NORMAL(""), SEQUENCE("$"), DBDELAY("@");
    private final String symbol;

    private DBConfType(String symbol) {
        this.symbol = symbol;
    }

    public String getSymbol() {
        return symbol;
    }

    public static DBConfType getType(String line) {
        if (line == null || StringUtils.isEmpty(line.trim())) {
            return NONE;
        }
        line = line.trim();
        if (line.startsWith(NONE.symbol)) {
            return NONE;
        }
        if (line.startsWith(SEQUENCE.symbol)) {
            return SEQUENCE;
        }
        if (line.startsWith(DBDELAY.symbol)) {
            return DBDELAY;
        }
        return NORMAL;
    }
}
