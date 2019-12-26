package me.ele.jarch.athena.server.async;

import io.netty.channel.ChannelHandlerContext;
import me.ele.jarch.athena.pg.proto.StartupMessage;
import me.ele.jarch.athena.sql.CmdTcpPacket;
import me.ele.jarch.athena.sql.PGCmdTcpPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PGAsyncSqlServerPacketDecoder extends AsyncSqlServerPacketDecoder {
    private static final Logger LOGGER =
        LoggerFactory.getLogger(PGAsyncSqlServerPacketDecoder.class);

    @Override protected CmdTcpPacket newPacket() {
        return new PGCmdTcpPacket(inputbuf);
    }

    @Override public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        StartupMessage msg = new StartupMessage(session.getDbConnInfo().getUser(),
            session.getDbConnInfo().getDatabase());
        session.dbServerWriteAndFlush(msg.toPacket(), (future) -> {
            if (future.isSuccess()) {
                return;
            }
            LOGGER.error("Cannot send StartupMessage to server in channelActive. " + this.session
                .getDbConnInfo().getQualifiedDbId());
            session.doQuit("Cannot send StartupMessage to server in channelActive. ");
        });
    }
}
