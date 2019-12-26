package me.ele.jarch.athena.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.constant.Metrics;
import me.ele.jarch.athena.scheduler.ClientLoginMonitor;
import me.ele.jarch.athena.scheduler.DBChannelDispatcher;
import me.ele.jarch.athena.scheduler.HangSessionMonitor;
import me.ele.jarch.athena.util.etrace.MetricFactory;
import me.ele.jarch.athena.worker.SchedulerWorker;
import me.ele.jarch.athena.worker.manager.ClientDecoderManager;
import me.ele.jarch.athena.worker.manager.ClientInactiveManager;
import me.ele.jarch.athena.worker.manager.QuitPacketManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class SqlClientPacketDecoder extends AbstractSqlPacketDecoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlClientPacketDecoder.class);

    public volatile DBChannelDispatcher holder = null;

    protected final SqlSessionContext sqlCtx;

    protected SqlClientPacketDecoder(boolean bind2Master, String remoteAddr) {
        sqlCtx = newSqlSessionContext(bind2Master, remoteAddr);
    }

    protected abstract SqlSessionContext newSqlSessionContext(boolean bind2Master,
        String remoteAddr);

    /**
     * 当客户端与DAL建立连接onActive后,会执行一些行为
     * <p>
     * 由于MySQL是主动发起handshake,所以会有具体行为 而PG是被动接受客户端的handshake,所以没有具体行为
     */
    protected abstract void handshakeOnActive();

    protected abstract boolean isQuitPacket(byte[] src);

    @Override public void channelActive(ChannelHandlerContext ctx) throws Exception {
        sqlCtx.setClientChannel(ctx.channel());
        sqlCtx.bindClientPackets(packets);

        HangSessionMonitor.getInst().addCtx(sqlCtx);
        ClientLoginMonitor.getInstance().addSlowWarningJob(sqlCtx);

        handshakeOnActive();
        finalChannelActive(ctx);
    }

    protected void finalChannelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);

        // change from info level to debug level, the login success/failure log is enough
        // change to debug to avoid health check heartbeat connections logs
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("client channelActive: " + sqlCtx);
        }
        sqlCtx.sqlSessionContextUtil.appendLoginPhrase(Constants.CHANNEL_ACTIVE);
    }

    @Override public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        SchedulerWorker.getInstance().enqueue(new ClientInactiveManager(this.sqlCtx));
        super.channelInactive(ctx);
        if (!this.sqlCtx.authenticator.isDalHealthCheckUser
            && this.sqlCtx.authenticator.isAuthenticated) {
            long activeTime = (this.sqlCtx.sqlSessionContextUtil.getLastSQLTimeInMills()
                - this.sqlCtx.authenticator.birthdayInMill) / 1000;
            long liveTime =
                (System.currentTimeMillis() - this.sqlCtx.authenticator.birthdayInMill) / 1000;
            if (activeTime > HangSessionMonitor.LONG_LIVED_SESSION_LIMIT / 1000) {
                MetricFactory.newGaugeWithSqlSessionContext(Metrics.SESSION_LIVE_TIME, this.sqlCtx)
                    .value(activeTime);
                LOGGER.warn(this.sqlCtx.toString() + String
                    .format("[ client channelInactive : active time: %d, live time: %d", activeTime,
                        liveTime));
            }
        }
    }

    @Override protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out)
        throws Exception {
        inputbuf.writeBytes(msg);
        extractPackets(ctx);

        if (this.packets.isEmpty()) {
            return;
        }

        commentDecodeAndSet(packets.peek());

        // do metrics calculation
        sqlCtx.sqlSessionContextUtil.cRecentRecvTime = System.currentTimeMillis();
        byte[] packet = packets.peek();
        traceOnLoginPacketReceive(packet);
        if (isQuitPacket(packet)) {
            SchedulerWorker.getInstance().enqueue(new QuitPacketManager(sqlCtx));
        } else {
            SchedulerWorker.getInstance()
                .enqueue(new ClientDecoderManager(sqlCtx, clonePackets()).setHolder(holder));
        }
    }

    protected abstract void traceOnLoginPacketReceive(byte[] packet);

    protected void commentDecodeAndSet(byte[] packet) {
    }
}
