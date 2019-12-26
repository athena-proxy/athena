package me.ele.jarch.athena.pg.proto;

import java.util.Collections;
import java.util.List;

/**
 * Created by jinghao.wang on 16/11/26.
 */
public class Sync extends PGMessage {
    public static final Sync INSTANCE = new Sync();

    @Override protected List<byte[]> getPayload() {
        return Collections.emptyList();
    }

    @Override protected byte getTypeByte() {
        return PGFlags.C_SYNC;
    }

    private Sync() {
    }

    @Override public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Sync {}");
        return builder.toString();
    }

}
