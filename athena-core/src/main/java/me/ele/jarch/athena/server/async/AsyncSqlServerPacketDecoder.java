package me.ele.jarch.athena.server.async;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import me.ele.jarch.athena.netty.AbstractSqlPacketDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class AsyncSqlServerPacketDecoder extends AbstractSqlPacketDecoder {
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncSqlServerPacketDecoder.class);

    AsyncClient session;

    public void setServerSession(AsyncClient serverSession) {
        this.session = serverSession;
        this.session.setServerPackets(packets);
    }

    @Override protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out)
        throws Exception {
        /*
         * The reason of having this check is that we must make sure we have valid tcp packet comes in.
         * But why we can have a packet whose size is 0? This only happens to LocalChannel server and client.
         *
         * In the legacy athena's code, it needs to write a 0-length array of bytes to force the flush of
         * tcp buffer. In the real network environment, it is OK; it will flush out the buffer if there is
         * bytes in it, or it will do nothing if the buffer is empty.
         * But when using LocalChannel, the transport that we use is memory, every writeAndFlush of a netty
         * channel will trigger a channel decode event.
         *
         * So to avoid this fake channel active event, we have to check against the size of *msg*.
         * */
        if (msg.readableBytes() == 0) {
            return;
        }

        inputbuf.writeBytes(msg);

        extractPackets(ctx);

        session.execute();
    }

    @Override public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        try {
            if (session != null) {
                session.handleChannelInactive();
            }
        } catch (Exception e) {
            LOGGER.error("", e);
        }

        super.channelInactive(ctx);
    }

}
