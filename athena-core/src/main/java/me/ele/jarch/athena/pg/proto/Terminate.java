package me.ele.jarch.athena.pg.proto;

import java.util.Collections;
import java.util.List;

/**
 * Created by jinghao.wang on 16/11/26.
 */
public class Terminate extends PGMessage {
    public static final Terminate INSTANCE = new Terminate();

    @Override protected List<byte[]> getPayload() {
        return Collections.emptyList();
    }

    @Override protected byte getTypeByte() {
        return PGFlags.C_TERMINATE;
    }

    private Terminate() {
    }

    @Override public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Terminate {}");
        return builder.toString();
    }

}
