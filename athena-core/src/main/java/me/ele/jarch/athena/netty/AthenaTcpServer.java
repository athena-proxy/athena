package me.ele.jarch.athena.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollChannelOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class AthenaTcpServer {
    private static final Logger logger = LoggerFactory.getLogger(AthenaTcpServer.class);
    private final ServerBootstrap bootstrap;
    private Map<String, Channel> bossChannels = new HashMap<>();
    private AtomicInteger listenedPortsCount = new AtomicInteger(0);

    /**
     * @param tryReusePort 是否尝试使用reuse_port机制, 该参数在8844的端口绑定时为false
     */
    public AthenaTcpServer(ChannelInitializer<Channel> initializer, EventLoopGroup eventLoopGroup,
        boolean tryReusePort) {
        bootstrap = AthenaEventLoopGroupCenter.newServerBootstrap(eventLoopGroup);
        bootstrap.option(ChannelOption.SO_BACKLOG, 512);
        if (tryReusePort && AthenaUtils.isOSKernelSupportReuseport() && Epoll.isAvailable()) {
            bootstrap.option(EpollChannelOption.SO_REUSEPORT, true);
        }
        bootstrap.childOption(ChannelOption.SO_KEEPALIVE, true)
            .childOption(ChannelOption.TCP_NODELAY, true);
        bootstrap.childHandler(initializer);
    }

    public ChannelFuture tryStart(int port) throws Exception {
        ChannelFuture bindFuture = bindToPort(port);
        return bindFuture.sync();
    }

    private ChannelFuture bindToPort(int port) {
        ChannelFuture future = bootstrap.bind(port);
        Channel oldChannel = bossChannels.put(String.format("0.0.0.0:%d", port), future.channel());
        listenedPortsCount.incrementAndGet();
        if (Objects.nonNull(oldChannel)) {
            oldChannel.close();
        }
        return future;
    }

    public void closeBossChannel() throws InterruptedException {
        for (Map.Entry<String, Channel> entry : bossChannels.entrySet()) {
            entry.getValue().close().sync();
            listenedPortsCount.decrementAndGet();
            logger.warn(AthenaUtils.pidStr() + ": " + entry.getKey() + " server channel closed");
        }
    }

    public int getListenedPortsCount() {
        return listenedPortsCount.get();
    }
}
