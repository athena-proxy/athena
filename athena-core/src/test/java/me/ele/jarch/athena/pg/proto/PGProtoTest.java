package me.ele.jarch.athena.pg.proto;

import org.testng.annotations.Test;

import java.util.Arrays;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Created by jinghao.wang on 16/12/16.
 */
public class PGProtoTest {
    //@formatter:off
    private byte unsignedMaxByte = (byte) 0b11111111;
    private byte unsignedByte0   = (byte) 0b00000000;
    private byte unsignedByte1   = (byte) 0b00000001;
    private byte unsignedByte2   = (byte) 0b00000010;
    @SuppressWarnings("unused")
    private byte unsignedByte4   = (byte) 0b00000100;
    @SuppressWarnings("unused")
    private byte unsignedByte8   = (byte) 0b00001000;
    @SuppressWarnings("unused")
    private byte unsignedByte16  = (byte) 0b00010000;
    @SuppressWarnings("unused")
    private byte unsignedByte32  = (byte) 0b00100000;
    @SuppressWarnings("unused")
    private byte unsignedByte64  = (byte) 0b01000000;
    private byte unsignedByte128 = (byte) 0b10000000;
    //@formatter:on

    @Test public void testReadMaxUnsignedInt32() throws Exception {
        byte[] maxUnsignedInt =
            new byte[] {unsignedMaxByte, unsignedMaxByte, unsignedMaxByte, unsignedMaxByte};
        PGProto proto = new PGProto(maxUnsignedInt, 0);
        int actualMaxUnsignedInt = proto.readInt32();
        int expectedInt = -1;
        assertEquals(actualMaxUnsignedInt, expectedInt);
    }

    @Test public void testReadMinUnsignedInt32() {
        byte[] minUnsginedInt =
            new byte[] {unsignedByte0, unsignedByte0, unsignedByte0, unsignedByte0};
        PGProto proto = new PGProto(minUnsginedInt, 0);
        int actualMinUnsignedInt = proto.readInt32();
        int expectedInt = 0;
        assertEquals(actualMinUnsignedInt, expectedInt);
    }

    @Test public void testReadPostiveInt32() {
        byte[] postiveInt1 =
            new byte[] {unsignedByte0, unsignedByte0, unsignedByte0, unsignedByte1};
        PGProto proto = new PGProto(postiveInt1, 0);
        int actualInt = proto.readInt32();
        int expectedInt = 1;
        assertEquals(actualInt, expectedInt);
    }

    @Test public void testReadNegativeInt32() {
        byte[] negativeInt1 =
            new byte[] {unsignedMaxByte, unsignedMaxByte, unsignedMaxByte, unsignedByte2};
        PGProto proto = new PGProto(negativeInt1, 0);
        int actualInt = proto.readInt32();
        int expectedInt = -254;
        assertEquals(actualInt, expectedInt);
    }

    @Test public void testBuildMaxUnsignedInt32() {
        byte[] actalBytes = PGProto.buildInt32BE(-1);
        byte[] expectedBytes =
            new byte[] {unsignedMaxByte, unsignedMaxByte, unsignedMaxByte, unsignedMaxByte};
        assertTrue(Arrays.equals(actalBytes, expectedBytes));
    }

    @Test public void testBuildMinUnsignedInt32() {
        byte[] actalBytes = PGProto.buildInt32BE(0);
        byte[] expectedBytes =
            new byte[] {unsignedByte0, unsignedByte0, unsignedByte0, unsignedByte0};
        assertTrue(Arrays.equals(actalBytes, expectedBytes));
    }

    @Test public void testBuildPostiveInt32() {
        byte[] actalBytes = PGProto.buildInt32BE(1);
        byte[] expectedBytes =
            new byte[] {unsignedByte0, unsignedByte0, unsignedByte0, unsignedByte1};
        assertTrue(Arrays.equals(actalBytes, expectedBytes));
    }

    @Test public void testBuildNegativeInt32() {
        byte[] actalBytes = PGProto.buildInt32BE(-254);
        byte[] expectedBytes =
            new byte[] {unsignedMaxByte, unsignedMaxByte, unsignedMaxByte, unsignedByte2};
        assertTrue(Arrays.equals(actalBytes, expectedBytes));
    }

    @Test public void testBuildMaxSignedInt32() {
        byte[] actalBytes = PGProto.buildInt32BE(Integer.MAX_VALUE);
        byte[] expectedBytes =
            new byte[] {(byte) 0b01111111, unsignedMaxByte, unsignedMaxByte, unsignedMaxByte};
        assertTrue(Arrays.equals(actalBytes, expectedBytes));
    }

    @Test public void testBuildMinSignedInt32() {
        byte[] actalBytes = PGProto.buildInt32BE(Integer.MIN_VALUE);
        byte[] expectedBytes =
            new byte[] {unsignedByte128, unsignedByte0, unsignedByte0, unsignedByte0};
        assertTrue(Arrays.equals(actalBytes, expectedBytes));
    }
}
