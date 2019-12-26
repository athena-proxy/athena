package me.ele.jarch.athena.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalEventLoopGroup;
import io.netty.channel.local.LocalServerChannel;
import me.ele.jarch.athena.constant.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Created by zhengchao on 16/8/26.
 */
public class LocalChannelServer {
    private static final Logger logger = LoggerFactory.getLogger(LocalChannelServer.class);
    private static LocalEventLoopGroup eventLoopGroup;
    private Channel serverChannel = null;
    private static LocalChannelServer INSTANCE = new LocalChannelServer();

    private LocalChannelServer() {
    }

    public static LocalChannelServer getInstance() {
        return INSTANCE;
    }

    public void start() throws InterruptedException {
        if (Objects.nonNull(serverChannel) && serverChannel.isOpen()) {
            logger.error("local server has already been open:" + serverChannel.toString());
            return;
        }
        eventLoopGroup = new LocalEventLoopGroup();
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(eventLoopGroup);
        serverBootstrap.channel(LocalServerChannel.class);
        serverBootstrap.childHandler(new LocalChannelServerChInitializer());

        LocalAddress address = new LocalAddress(Constants.LOCAL_CHANNEL_SERVER_ADDRESS);
        ChannelFuture future =
            serverBootstrap.bind(address).addListener(new ChannelFutureListener() {
                @Override public void operationComplete(ChannelFuture future) throws Exception {
                    logger.info("local server successfully binds address "
                        + Constants.LOCAL_CHANNEL_SERVER_ADDRESS);
                }
            });

        serverChannel = future.channel();
    }

    public void shutdown() {
        if (Objects.nonNull(serverChannel)) {
            serverChannel.close().addListener(new ChannelFutureListener() {
                @Override public void operationComplete(ChannelFuture future) throws Exception {
                    logger.info("local server has been closed");
                }
            });
            serverChannel = null;
        }

        eventLoopGroup.shutdownGracefully();
    }

    public boolean isOpen() {
        return serverChannel != null;
    }
}
