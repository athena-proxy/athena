package me.ele.jarch.athena.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.local.LocalChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zhengchao on 16/8/26.
 */
public class LocalChannelServerChInitializer extends ChannelInitializer<Channel> {
    private static final Logger logger =
        LoggerFactory.getLogger(LocalChannelServerChInitializer.class);

    @Override public void initChannel(Channel ch) throws Exception {
        ensureValidChannel(ch);

        try {
            ch.pipeline()
                .addLast(new LocalChannelClientPacketDecoder(false, ch.remoteAddress().toString()));
        } catch (Exception e) {
            logger.error("failed to initialize channel on local server:" + ch.localAddress(), e);
        }
    }

    private void ensureValidChannel(Channel ch) throws Exception {
        if (!(ch instanceof LocalChannel)) {
            throw new Exception("channel type incorrect, only LocalChannel supported");
        }
    }
}
