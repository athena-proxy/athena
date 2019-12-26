package me.ele.jarch.athena.netty;

import io.netty.channel.ChannelHandlerContext;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.pg.proto.PGFlags;
import me.ele.jarch.athena.sql.CmdTcpPacket;
import me.ele.jarch.athena.sql.PGCmdTcpPacket;
import me.ele.jarch.athena.util.NoThrow;
import me.ele.jarch.athena.util.PacketLengthUtil;

public class PGSqlClientPacketDecoder extends SqlClientPacketDecoder {

    public PGSqlClientPacketDecoder(boolean bind2Master, String remoteAddr) {
        super(bind2Master, remoteAddr);
    }

    @Override
    protected SqlSessionContext newSqlSessionContext(boolean bind2Master, String remoteAddr) {
        return new PGSqlSessionContext(this, bind2Master, remoteAddr);
    }

    @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
        throws Exception {
        PacketLengthUtil.trySendErr2Client(sqlCtx, cause, () -> PacketLengthUtil
            .buildPGPktTooLargeErr(getClass().getSimpleName(), "client_packet_decode").toPacket());
        super.exceptionCaught(ctx, cause);
    }

    @Override protected void handshakeOnActive() {
    }

    @Override protected CmdTcpPacket newPacket() {
        return new PGCmdTcpPacket(inputbuf);
    }

    @Override protected boolean isQuitPacket(byte[] src) {
        if (src == null) {
            return false;
        }
        if (src.length < 5) {
            return false;
        }
        return src[0] == PGFlags.C_TERMINATE && src[4] == 4;
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

    @Override protected void traceOnLoginPacketReceive(byte[] packet) {
        if (packet[0] == PGFlags.C_SSL_REQUEST) {
            sqlCtx.sqlSessionContextUtil.appendLoginPhrase(Constants.PG_SSL_RECEIVED);
        } else if (packet[0] == PGFlags.C_STARTUP_MESSAGE) {
            sqlCtx.sqlSessionContextUtil.appendLoginPhrase(Constants.PG_STARTUP_RECEIVED);
        } else if (packet[0] == PGFlags.C_PASSWORD_MESSAGE) {
            sqlCtx.sqlSessionContextUtil.appendLoginPhrase(Constants.PG_AUTH_RESPONSE_RECEIVED);
        }
    }
}
