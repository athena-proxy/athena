package me.ele.jarch.athena.netty.upgrade;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.channel.unix.DomainSocketChannel;
import io.netty.util.CharsetUtil;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.constant.Metrics;
import me.ele.jarch.athena.constant.TraceNames;
import me.ele.jarch.athena.netty.AthenaUtils;
import me.ele.jarch.athena.netty.upgrade.UpgradeCenter.STATUS;
import me.ele.jarch.athena.util.etrace.MetricFactory;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * @author shaoyang.qi
 * <p>
 * unix domain socket server
 * <p>
 * 用于和unix domain socket client通信
 */
public class UdsServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(UdsServer.class);
    private static final String SUCCESS = "exec_sucess";
    private static final String FAILURE = "exec_fail";
    private final String listenPath;
    private EventLoopGroup group = null;
    private volatile ChannelFuture cf = null;

    public UdsServer(String listenPath) {
        this.listenPath = Objects.requireNonNull(listenPath,
            () -> AthenaUtils.pidStr() + " listenPath of UdsServer should not be null!");
    }

    public void stop() throws InterruptedException {
        if (cf == null) {
            return;
        }
        try {
            cf.channel().close().sync();
            group.shutdownGracefully().sync();
        } finally {
            LOGGER.info(AthenaUtils.pidStr() + " UdsServer stopped!");
        }
    }

    public void start() throws Exception {
        try {
            group = new EpollEventLoopGroup(1);
            ServerBootstrap b = new ServerBootstrap();
            //@formatter:off
            b.group(group)
             .channel(EpollServerDomainSocketChannel.class)
             .childHandler(new ChannelInitializer<DomainSocketChannel>() {
                @Override
                protected void initChannel(DomainSocketChannel ch) throws Exception {
                     ch.pipeline().addLast(new UdsServerHandler());                
                }
              });
            //@formatter:on
            cf = b.bind(new DomainSocketAddress(listenPath)).sync();
        } catch (Exception e) {
            stop();
            throw e;
        }
    }

    @Sharable private class UdsServerHandler extends SimpleChannelInboundHandler<ByteBuf> {

        @Override public void channelActive(ChannelHandlerContext ctx) throws Exception {
            LOGGER.info(AthenaUtils.pidStr() + " uds server channel active!");
            super.channelActive(ctx);
        }

        @Override public void channelRead0(ChannelHandlerContext ctx, ByteBuf msg)
            throws Exception {
            String cmdStr = msg.toString(CharsetUtil.UTF_8);
            if (StringUtils.isNotEmpty(cmdStr)) {
                cmdStr = cmdStr.replaceAll("\n", "");
            }
            LOGGER.info(AthenaUtils.pidStr() + " uds server received: " + cmdStr);
            UdsCmd udsCmd = UdsCmd.getType(cmdStr);
            String result = SUCCESS;
            switch (udsCmd) {
                case CLOSE_UPGRADE_UDS:
                    MetricFactory.newCounter(Metrics.DAL_UPGRADE).addTag("Role", Constants.ROLE)
                        .addTag(TraceNames.STATUS, String.valueOf(STATUS.CLOSE_UPGRADE_UDS)).once();
                    UpgradeCenter.getInstance().finishUpgrade();
                    break;
                case CLOSE_LISTEN_PORTS:
                    MetricFactory.newCounter(Metrics.DAL_UPGRADE).addTag("Role", Constants.ROLE)
                        .addTag(TraceNames.STATUS, String.valueOf(STATUS.CLOSE_LISTEN_PORTS))
                        .once();
                    UpgradeCenter.getInstance().startDowngrade();
                    break;
                default:
                    MetricFactory.newCounter(Metrics.DAL_UPGRADE).addTag("Role", Constants.ROLE)
                        .addTag(TraceNames.STATUS, String.valueOf(STATUS.FAILED)).once();
                    result = FAILURE;
            }
            ctx.writeAndFlush(Unpooled.wrappedBuffer(result.getBytes()))
                .addListener(ChannelFutureListener.CLOSE);
        }

        @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
            throws Exception {
            LOGGER.error(AthenaUtils.pidStr() + " uds server channel catch exception.", cause);
            MetricFactory.newCounter(Metrics.DAL_UPGRADE).addTag("Role", Constants.ROLE)
                .addTag(TraceNames.STATUS, String.valueOf(STATUS.FAILED)).once();
            ctx.writeAndFlush(Unpooled.wrappedBuffer(FAILURE.getBytes()))
                .addListener(ChannelFutureListener.CLOSE);
        }
    }
}
