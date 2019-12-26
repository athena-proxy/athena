package me.ele.jarch.athena.util;

import com.github.mpjct.jmpjct.util.ErrorCode;
import io.netty.handler.codec.DecoderException;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.exception.PacketTooLargeException;
import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.pg.proto.ErrorResponse;
import me.ele.jarch.athena.pg.util.Severity;
import org.testng.Assert;
import org.testng.annotations.Test;

public class PacketLengthUtilTest {

    @Test public void testBuildPGPktTooLargeErr() {
        String expectedFile = "file";
        String expectedRoutine = "routine";
        ErrorResponse expectedResponse =
            new ErrorResponse(Severity.FATAL, ErrorCode.DAL_PACKET_TOO_LARGE.getSqlState(),
                Constants.DAL + PacketLengthUtil.PKT_TOO_LARGE_ERR, expectedFile, "1",
                expectedRoutine);
        ErrorResponse actualResponse =
            PacketLengthUtil.buildPGPktTooLargeErr(expectedFile, expectedRoutine);
        Assert.assertEquals(actualResponse, expectedResponse);
    }

    @Test public void testCheckValidLength() {
        NoThrow.execute(() -> PacketLengthUtil.checkValidLength(16 * 1024 * 1024),
            (e) -> Assert.assertTrue(e instanceof PacketTooLargeException));
    }

    @Test public void testIsPacketTooLargeException() {
        PacketTooLargeException tooLargeExeption = new PacketTooLargeException("too large");
        QuitException quitException = new QuitException("quit");
        Assert.assertTrue(PacketLengthUtil.isPacketTooLargeException(tooLargeExeption));
        Assert.assertFalse(PacketLengthUtil.isPacketTooLargeException(quitException));
        DecoderException decoderTooLargeException = new DecoderException(tooLargeExeption);
        Assert.assertTrue(PacketLengthUtil.isPacketTooLargeException(decoderTooLargeException));
        DecoderException decoderQuitException = new DecoderException(quitException);
        Assert.assertFalse(PacketLengthUtil.isPacketTooLargeException(decoderQuitException));
    }
}
