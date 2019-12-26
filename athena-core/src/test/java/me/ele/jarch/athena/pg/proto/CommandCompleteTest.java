package me.ele.jarch.athena.pg.proto;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author shaoyang.qi
 */
public class CommandCompleteTest {
    @Test public void testToPacket() {
        String tag = "CommandComplelte";
        CommandComplete cmdCpl = new CommandComplete(tag);
        byte[] bytes = cmdCpl.toPacket();

        int expectedTotalLength = 1 + 4 + tag.length() + 1;
        assertEquals(bytes.length, expectedTotalLength);

        PGProto proto = new PGProto(bytes);
        byte expectedType = PGFlags.COMMAND_COMPLETE;
        assertEquals(proto.readByte(), expectedType);
        int expectedPayloadLength = expectedTotalLength - 1;
        assertEquals(proto.readInt32(), expectedPayloadLength);
        assertEquals(proto.readNullStr(), tag);
    }

    @Test public void testLoadFromPacket() {
        String tag = "CommandComplelte";
        CommandComplete cmdCpl = new CommandComplete(tag);
        byte[] bytes = cmdCpl.toPacket();

        CommandComplete newCmdCpl = CommandComplete.loadFromPacket(bytes);
        assertEquals(newCmdCpl.toPacket(), bytes);
        assertEquals(newCmdCpl.getTag(), cmdCpl.getTag());
    }
}
