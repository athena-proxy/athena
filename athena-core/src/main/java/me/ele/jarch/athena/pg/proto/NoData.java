package me.ele.jarch.athena.pg.proto;

import java.util.Collections;
import java.util.List;

/**
 * @author shaoyang.qi
 */
public class NoData extends PGMessage {
    public static final NoData INSTANCE = new NoData();

    @Override protected byte getTypeByte() {
        return PGFlags.NO_DATA;
    }

    @Override protected List<byte[]> getPayload() {
        return Collections.emptyList();
    }

    private NoData() {
    }

    @Override public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("NoData {}");
        return builder.toString();
    }

}
