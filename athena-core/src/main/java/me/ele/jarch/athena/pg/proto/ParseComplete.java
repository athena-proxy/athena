package me.ele.jarch.athena.pg.proto;

import java.util.Collections;
import java.util.List;

/**
 * @author shaoyang.qi
 */
public class ParseComplete extends PGMessage {
    public static final ParseComplete INSTANCE = new ParseComplete();

    @Override protected byte getTypeByte() {
        return PGFlags.PARSE_COMPLETE;
    }

    @Override protected List<byte[]> getPayload() {
        return Collections.emptyList();
    }

    private ParseComplete() {
    }

    @Override public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("ParseComplete {}");
        return builder.toString();
    }

}
