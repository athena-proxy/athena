package me.ele.jarch.athena.pg.proto;

import java.util.Collections;
import java.util.List;

/**
 * @author shaoyang.qi
 */
public class EmptyQueryResponse extends PGMessage {
    public static final EmptyQueryResponse INSTANCE = new EmptyQueryResponse();

    @Override protected byte getTypeByte() {
        return PGFlags.EMPTY_QUERY_RESPONSE;
    }

    @Override protected List<byte[]> getPayload() {
        return Collections.emptyList();
    }

    private EmptyQueryResponse() {
    }

    @Override public String toString() {
        return "EmptyQueryResponse{}";
    }
}
