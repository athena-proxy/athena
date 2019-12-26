package me.ele.jarch.athena.pg.proto;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Created by jinghao.wang on 16/11/24.
 */
public class ReadyForQuery extends PGMessage {
    public static final byte IDLE_BYTE = 'I';
    public static final byte IN_TX_BYTE = 'T';
    public static final byte IN_FAILED_TX_BYTE = 'E';

    public static final ReadyForQuery IDLE = new ReadyForQuery((byte) 'I');
    public static final ReadyForQuery IN_TX = new ReadyForQuery((byte) 'T');
    public static final ReadyForQuery IN_FAILED_TX = new ReadyForQuery((byte) 'E');

    /**
     * @formatter:off only accept
     * 'I' -> Idle
     * 'T' -> In a transaction block
     * 'E' -> In failed transaction block
     * @formatter:on
     */
    private final byte status;

    @Override protected List<byte[]> getPayload() {
        return Collections.singletonList(new byte[] {status});
    }

    @Override protected byte getTypeByte() {
        return PGFlags.READY_FOR_QUERY;
    }

    ReadyForQuery(byte status) {
        this.status = status;
    }

    public byte getStatus() {
        return status;
    }

    public static ReadyForQuery loadFromPacket(byte[] packet) {
        byte status = packet[packet.length - 1];
        switch (status) {
            case IDLE_BYTE:
                return IDLE;
            case IN_TX_BYTE:
                return IN_TX;
            case IN_FAILED_TX_BYTE:
                return IN_FAILED_TX;
            default:
                throw new IllegalArgumentException(
                    "illegal ReadyForQuery packet " + Arrays.toString(packet));
        }
    }

    @Override public String toString() {
        final StringBuilder sb = new StringBuilder("ReadyForQuery{");
        sb.append("status=").append((char) status);
        sb.append('}');
        return sb.toString();
    }
}
