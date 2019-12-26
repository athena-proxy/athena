package me.ele.jarch.athena.pg.proto;

import java.util.Collections;
import java.util.List;

/**
 * @author shaoyang.qi
 */
public class CloseComplete extends PGMessage {
    public static final CloseComplete INSTANCE = new CloseComplete();

    @Override protected byte getTypeByte() {
        return PGFlags.CLOSE_COMPLETE;
    }

    @Override protected List<byte[]> getPayload() {
        return Collections.emptyList();
    }

    private CloseComplete() {
    }

    @Override public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("CloseComplete {}");
        return builder.toString();
    }

}
