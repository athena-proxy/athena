package me.ele.jarch.athena.util;

import com.github.mpjct.jmpjct.util.ErrorCode;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.DecoderException;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.exception.PacketTooLargeException;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.pg.proto.ErrorResponse;
import me.ele.jarch.athena.pg.util.Severity;

import java.util.function.Supplier;

public class PacketLengthUtil {
    public static final String PKT_TOO_LARGE_ERR =
        "the length of the packet is too large,over 16M!";

    public static void checkValidLength(int length) throws PacketTooLargeException {
        if (length < Constants.MAX_PACKET_SIZE) {
            return;
        }
        // 如果包长度大于16M，直接断连接
        throw new PacketTooLargeException(PKT_TOO_LARGE_ERR);
    }

    public static boolean isValidPayload(byte[] payload) {
        return payload != null;
    }

    public static void trySendErr2Client(SqlSessionContext sqlCtx, Throwable cause,
        Supplier<byte[]> errMsg) {
        if (isPacketTooLargeException(cause)) {
            sqlCtx.clientWriteAndFlush(Unpooled.wrappedBuffer(errMsg.get()));
        }
    }

    public static boolean isPacketTooLargeException(Throwable cause) {
        if (cause instanceof PacketTooLargeException) {
            return true;
        }
        if (cause instanceof DecoderException) {
            return isPacketTooLargeException(cause.getCause());
        }
        return false;
    }

    public static ErrorResponse buildPGPktTooLargeErr(String file, String routine) {
        return ErrorResponse
            .buildErrorResponse(Severity.FATAL, ErrorCode.DAL_PACKET_TOO_LARGE.getSqlState(),
                PKT_TOO_LARGE_ERR, file, "1", routine);
    }
}
