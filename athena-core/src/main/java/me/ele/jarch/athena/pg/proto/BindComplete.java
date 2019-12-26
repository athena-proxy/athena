package me.ele.jarch.athena.pg.proto;

import java.util.Collections;
import java.util.List;

/**
 * @author shaoyang.qi
 */
public class BindComplete extends PGMessage {
    public static final BindComplete INSTANCE = new BindComplete();

    @Override protected byte getTypeByte() {
        return PGFlags.BIND_COMPLETE;
    }

    @Override protected List<byte[]> getPayload() {
        return Collections.emptyList();
    }

    private BindComplete() {
    }

    @Override public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("BindComplete {}");
        return builder.toString();
    }

}
