package me.ele.jarch.athena.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.local.LocalEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.util.Objects;

public class AthenaEventLoopGroupCenter {

    // 连接debug页面的boss和worker共用该EventLoopGroup
    private static final EventLoopGroup debugServerGroup = Epoll.isAvailable() ?
        new EpollEventLoopGroup(1, new DefaultThreadFactory("epollDebug")) :
        new NioEventLoopGroup(1, new DefaultThreadFactory("nioDebug"));

    // 所有连接客户端和服务端(包括心跳)的boss和worker共用该EventLoopGroup,在未初始化时使用debug的
    private static EventLoopGroup serverGroup = debugServerGroup;
    private static EventLoopGroup clientGroup = debugServerGroup;
    private static EventLoopGroup asyncGroup = debugServerGroup;

    // TODO for legacy local channel
    private static final EventLoopGroup localWorkGroup = new LocalEventLoopGroup(1);

    private static final Class<? extends Channel> CHANNEL_CLASS =
        Epoll.isAvailable() ? EpollSocketChannel.class : NioSocketChannel.class;

    private static final Class<? extends ServerChannel> SERVER_CHANNEL_CLASS =
        Epoll.isAvailable() ? EpollServerSocketChannel.class : NioServerSocketChannel.class;

    public static EventLoopGroup getSeverWorkerGroup() {
        Objects.requireNonNull(serverGroup,
            "serverGroup is null, please make sure AthenaEventLoopGroupCenter.init() is called");
        return serverGroup;
    }

    public static EventLoopGroup getClientWorkerGroup() {
        Objects.requireNonNull(clientGroup,
            "clientGroup is null, please make sure AthenaEventLoopGroupCenter.init() is called");
        return clientGroup;
    }

    public static EventLoopGroup getAsyncWorkerGroup() {
        Objects.requireNonNull(asyncGroup,
            "asyncGroup is null, please make sure AthenaEventLoopGroupCenter.init() is called");
        return asyncGroup;
    }

    public static EventLoopGroup getLocalWorkerGroup() {
        return localWorkGroup;
    }

    public static EventLoopGroup getDebugServerGroup() {
        return debugServerGroup;
    }

    public static void init(int workerThreads, int asyncThreads) {
        serverGroup = Epoll.isAvailable() ?
            new EpollEventLoopGroup(workerThreads, new DefaultThreadFactory("epollServer")) :
            new NioEventLoopGroup(workerThreads, new DefaultThreadFactory("nioServer"));
        clientGroup = Epoll.isAvailable() ?
            new EpollEventLoopGroup(workerThreads, new DefaultThreadFactory("epollClient")) :
            new NioEventLoopGroup(workerThreads, new DefaultThreadFactory("nioClient"));
        asyncGroup = Epoll.isAvailable() ?
            new EpollEventLoopGroup(asyncThreads, new DefaultThreadFactory("epollAsync")) :
            new NioEventLoopGroup(asyncThreads, new DefaultThreadFactory("nioAsync"));
    }

    // 用于客户端的channel class
    public static Class<? extends Channel> getChannelClass() {
        return CHANNEL_CLASS;
    }

    public static ServerBootstrap newServerBootstrap(EventLoopGroup eventLoopGroup) {
        return new ServerBootstrap().group(eventLoopGroup).channel(SERVER_CHANNEL_CLASS);
    }
}
