package me.ele.jarch.athena.netty.debug;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;

public class DebugChInitializer extends ChannelInitializer<Channel> {

    @Override public void initChannel(Channel ch) throws Exception {
        // server端发送的是httpResponse，所以要使用HttpResponseEncoder进行编码
        ch.pipeline().addLast(new HttpResponseEncoder());
        // server端接收到的是httpRequest，所以要使用HttpRequestDecoder进行解码
        ch.pipeline().addLast(new HttpRequestDecoder());
        ch.pipeline().addLast(new HttpServerInboundHandler());
    }
}
