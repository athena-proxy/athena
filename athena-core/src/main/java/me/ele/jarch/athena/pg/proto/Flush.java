package me.ele.jarch.athena.pg.proto;

import java.util.Collections;
import java.util.List;

/**
 * @author shaoyang.qi
 */
public class Flush extends PGMessage {
    public static final Flush INSTANCE = new Flush();

    @Override protected byte getTypeByte() {
        return PGFlags.C_FLUSH;
    }

    @Override protected List<byte[]> getPayload() {
        return Collections.emptyList();
    }

    private Flush() {
    }

    @Override public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Flush {}");
        return builder.toString();
    }

}
