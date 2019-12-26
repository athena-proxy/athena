package me.ele.jarch.athena.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import me.ele.jarch.athena.constant.Metrics;
import me.ele.jarch.athena.server.pool.ServerSession;
import me.ele.jarch.athena.server.pool.ServerSession.SERVER_SESSION_STATUS;
import me.ele.jarch.athena.util.etrace.MetricFactory;
import me.ele.jarch.athena.worker.SchedulerWorker;
import me.ele.jarch.athena.worker.manager.ServerDecoderManager;
import me.ele.jarch.athena.worker.manager.ServerInactiveManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public abstract class SqlServerPacketDecoder extends AbstractSqlPacketDecoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerPacketDecoder.class);

    protected ServerSession serverSession;

    protected volatile SqlSessionContext sqlCtx;

    public void setSqlCtx(SqlSessionContext sqlCtx) {
        this.sqlCtx = sqlCtx;
    }

    public void setServerSession(ServerSession serverSession) {
        this.serverSession = serverSession;
        this.serverSession.setServerPackets(packets);
    }

    @Override public void channelActive(ChannelHandlerContext ctx) throws Exception {
        serverSession.setServerChannel(ctx.channel());
        super.channelActive(ctx);
    }

    @Override public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        SqlSessionContext context = this.sqlCtx;
        try {
            if (!isIdle(context)) {
                SchedulerWorker.getInstance()
                    .enqueue(new ServerInactiveManager(context, serverSession));
            } else {
                serverSession.setStatus(SERVER_SESSION_STATUS.QUIT);
                serverSession.execute();
                LOGGER.info(
                    "Server channelInactive SqlSessionContext = null, server session {} packets {}",
                    this.serverSession, Arrays.toString(packets.peek()));
                MetricFactory.newCounterWithServerSession(Metrics.CONN_SERVER_BROKEN, serverSession)
                    .once();
            }
        } finally {
            super.channelInactive(ctx);
        }
    }

    private boolean isIdle(SqlSessionContext context) {
        return context == null;
    }

    @Override protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out)
        throws Exception {
        inputbuf.writeBytes(msg);
        extractPackets(ctx);

        if (this.packets.isEmpty()) {
            return;
        }

        if (!this.serverSession.isReady()) {
            serverSession.execute();
            return;
        }
        if (sqlCtx != null) {
            SchedulerWorker.getInstance()
                .enqueue(new ServerDecoderManager(sqlCtx, clonePackets(), serverSession));
        } else {
            recvPacketWhenNoSqlCtxBind();
        }
    }

    /**
     * 该方法在数据库端有数据,但此时没有客户端与之绑定的情况下的行为
     * <p>
     * MySQL的行为是报错
     * PG的行为是打印日志
     */
    protected abstract void recvPacketWhenNoSqlCtxBind();

    public SqlSessionContext getSqlCtx() {
        return sqlCtx;
    }
}
