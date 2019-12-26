package me.ele.jarch.athena.pg.proto;

import java.util.Collections;
import java.util.List;

/**
 * @author shaoyang.qi
 */
public class PortalSuspended extends PGMessage {
    public static final PortalSuspended INSTANCE = new PortalSuspended();

    @Override protected byte getTypeByte() {
        return PGFlags.PORTAL_SUSPENDED;
    }

    @Override protected List<byte[]> getPayload() {
        return Collections.emptyList();
    }

    private PortalSuspended() {
    }

    @Override public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("PortalSuspended {}");
        return builder.toString();
    }

}
