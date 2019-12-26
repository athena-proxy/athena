package me.ele.jarch.athena.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import me.ele.jarch.athena.constant.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public class AthenaChInitializer extends ChannelInitializer<Channel> {
    private static final Logger logger = LoggerFactory.getLogger(AthenaChInitializer.class);

    @Override public void initChannel(Channel channel) throws Exception {
        InetSocketAddress address = (InetSocketAddress) channel.localAddress();
        try {
            int port = address.getPort();
            if (isPGServicePort(port)) {
                channel.pipeline().addLast(new PGHeadingPacketDecoder());
                channel.pipeline().addLast(new PGSqlClientPacketDecoder(isBind2Master(port),
                    channel.remoteAddress().toString()));
                return;
            }
            if (isMysqlServicePort(port)) {
                channel.pipeline().addLast(new MySqlClientPacketDecoder(isBind2Master(port),
                    channel.remoteAddress().toString()));
                return;
            }
            logger.error("cannot find the corresponding schedular for port " + address.getPort());
            channel.close();
        } catch (Exception e) {
            logger.error("Failed to initialize channel on server : " + address.getAddress(), e);
            throw e;
        }

    }

    private boolean isBind2Master(int port) {
        return port % 2 == 0;
    }

    private boolean isPGServicePort(int port) {
        return port < Constants.MAX_PG_SERVICE_PORT;
    }

    private boolean isMysqlServicePort(int port) {
        return port > Constants.MIN_MYSQL_SERVICE_PORT;
    }
}
