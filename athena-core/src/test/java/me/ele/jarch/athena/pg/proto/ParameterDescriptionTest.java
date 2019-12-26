package me.ele.jarch.athena.pg.proto;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author shaoyang.qi
 */
public class ParameterDescriptionTest {
    @Test public void testToPacket() {
        int[] paramIds = new int[] {23, 1043};
        ParameterDescription pd = new ParameterDescription(paramIds);
        byte[] bytes = pd.toPacket();

        int expectedTotalLength = 1 + 4 + 2 + 4 * paramIds.length;
        assertEquals(bytes.length, expectedTotalLength);

        PGProto proto = new PGProto(bytes);
        byte expectedType = PGFlags.PARAMETER_DESCRIPTION;
        assertEquals(proto.readByte(), expectedType);
        int expectedPayloadLength = expectedTotalLength - 1;
        assertEquals(proto.readInt32(), expectedPayloadLength);
        int expectedParamCount = paramIds.length;
        assertEquals(proto.readInt16(), expectedParamCount);
        for (int i = 0; i < expectedParamCount; i++) {
            assertEquals(proto.readInt32(), paramIds[i]);
        }
    }

    @Test public void testLoadFromPacket() {
        int[] paramIds = new int[] {23, 1043};
        ParameterDescription pd = new ParameterDescription(paramIds);
        byte[] bytes = pd.toPacket();

        ParameterDescription newPd = ParameterDescription.loadFromPacket(bytes);
        assertEquals(newPd.toPacket(), bytes);
        assertEquals(newPd.getParamCount(), paramIds.length);
        assertEquals(newPd.getParamIds(), paramIds);
    }
}
