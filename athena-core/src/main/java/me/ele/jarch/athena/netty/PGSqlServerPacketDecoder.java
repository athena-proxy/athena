package me.ele.jarch.athena.netty;

import io.netty.channel.ChannelHandlerContext;
import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.pg.proto.*;
import me.ele.jarch.athena.server.pool.PGServerSession;
import me.ele.jarch.athena.sql.CmdTcpPacket;
import me.ele.jarch.athena.sql.PGCmdTcpPacket;
import me.ele.jarch.athena.util.NoThrow;
import me.ele.jarch.athena.util.PacketLengthUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PGSqlServerPacketDecoder extends SqlServerPacketDecoder {
    private static final Logger LOGGER = LoggerFactory.getLogger(PGSqlServerPacketDecoder.class);

    @Override protected CmdTcpPacket newPacket() {
        return new PGCmdTcpPacket(inputbuf);
    }

    @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
        throws Exception {
        PacketLengthUtil.trySendErr2Client(sqlCtx, cause, () -> PacketLengthUtil
            .buildPGPktTooLargeErr(getClass().getSimpleName(), "db_packet_decode").toPacket());
        super.exceptionCaught(ctx, cause);
    }

    @Override public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        serverSession
            .dbServerWriteAndFlush(((PGServerSession) serverSession).newStartupMessage(), (s) -> {
                LOGGER.error(
                    "Cannot send StartupMessage to server in channelActive. " + this.serverSession
                        .getOriginDbConnInfo().getQualifiedDbId());
                serverSession.doQuit();
            });
    }

    // PG复写该方法,使得异步结果仍然存储在packets里
    @Override protected void recvPacketWhenNoSqlCtxBind() {
        try {
            for (byte[] packet : packets) {
                if (packet.length < 1) {
                    continue;
                }
                // 暂时使用ERROR级别输出,方便查看线上异步操作的情况
                // 待PG稳定后可以改成WARN或者INFO
                switch (packet[0]) {
                    case PGFlags.PARAMETER_STATUS:
                        LOGGER.error("Async ParameterStatus found: {} from {}",
                            ParameterStatus.loadFromPacket(packet), serverSession);
                        break;
                    case PGFlags.NOTICE_RESPONSE:
                        LOGGER.error("Async NoticeResponse found: {} from {}",
                            NoticeResponse.loadFromPacket(packet), serverSession);
                        break;
                    case PGFlags.NOTIFICATION_RESPONSE:
                        LOGGER.error("Async NotificationResponse found: {} from {}",
                            NotificationResponse.loadFromPacket(packet), serverSession);
                        break;
                    case PGFlags.ERROR_RESPONSE:
                        LOGGER.error("Async ErrorResponse found: {} from {}",
                            ErrorResponse.loadFromPacket(packet), serverSession);
                        break;
                    default:
                        LOGGER.error("Async Unknown Response found: {} from {}", (char) packet[0],
                            serverSession);
                        break;
                }
            }
        } catch (Exception e) {
            LOGGER.error("error when output packets when not sqlCtx bind", e);
        }

    }

    @Override
    protected void dealExtractPktQuitException(ChannelHandlerContext ctx, QuitException e) {
        if (PacketLengthUtil.isPacketTooLargeException(e)) {
            NoThrow.call(() -> this.exceptionCaught(ctx, e));
            // packet长度过大时，不再处理inputbuf,并丢弃已经解析的包
            inputbuf.clear();
            packets.clear();
            return;
        }
        super.dealExtractPktQuitException(ctx, e);
    }
}
