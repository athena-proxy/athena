package me.ele.jarch.athena.pg.proto;

import java.util.Collections;
import java.util.List;

/**
 * @author shaoyang.qi
 */
public class CommandComplete extends PGMessage {
    private String tag = "";

    @Override protected byte getTypeByte() {
        return PGFlags.COMMAND_COMPLETE;
    }

    @Override protected List<byte[]> getPayload() {
        return Collections.singletonList(PGProto.buildNullStr(tag));
    }

    public CommandComplete(String tag) {
        this.tag = tag;
    }

    public String getTag() {
        return tag;
    }

    public long getRows() {
        //@formatter:off
        if (tag.startsWith("INSERT") ||
            tag.startsWith("UPDATE") ||
            tag.startsWith("DELETE") ||
            tag.startsWith("MOVE")   ||
            tag.startsWith("SELECT") ||
            tag.startsWith("MOVE")   ||
            tag.startsWith("FETCH")  ||
            tag.startsWith("COPY")) {
            return Long.valueOf(tag.substring(1 + tag.lastIndexOf(' ')));
        }
        //@formatter:on
        return 0;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public static CommandComplete loadFromPacket(byte[] packet) {
        String tag = new PGProto(packet, 5).readNullStr();
        return new CommandComplete(tag);
    }

    @Override public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("CommandComplete {tag=");
        builder.append(tag);
        builder.append("}");
        return builder.toString();
    }

}
