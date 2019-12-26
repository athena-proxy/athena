package me.ele.jarch.athena.util.rmq;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import me.ele.jarch.athena.util.AthenaConfig;
import me.ele.jarch.athena.util.NoThrow;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * interact with amqp
 *
 * @author shaoyang.qi
 */
public class RmqSender {
    private static Logger logger = LoggerFactory.getLogger(RmqSender.class);
    String exchangeName = "dal_sql";
    String normalRoutingKey = "normal";
    String slowRoutingKey = "slow";
    private volatile Connection autoRecoveryConn = null;
    private volatile Channel autoRecoveryChannel = null;

    private void tryInitChannel() {
        if (autoRecoveryChannel != null) {
            return;
        }
        if (StringUtils.isEmpty(AthenaConfig.getInstance().getAmqpUri())) {
            return;
        }
        NoThrow.call(() -> {
            logger.info(
                "start to init amqp connection, uri : " + AthenaConfig.getInstance().getAmqpUri());
            if (autoRecoveryConn == null) {
                ConnectionFactory factory = new ConnectionFactory();
                factory.setUri(AthenaConfig.getInstance().getAmqpUri());
                autoRecoveryConn = factory.newConnection();
            }
            autoRecoveryChannel = autoRecoveryConn.createChannel();
            if (autoRecoveryChannel != null) {
                autoRecoveryChannel
                    .exchangeDeclare(exchangeName, BuiltinExchangeType.DIRECT, true, false, null);
            }
        });
    }

    public void resetChannel() {
        if (autoRecoveryChannel == null) {
            return;
        }
        Channel tempChannel = autoRecoveryChannel;
        Connection tempConn = autoRecoveryConn;
        autoRecoveryChannel = null;
        autoRecoveryConn = null;
        NoThrow.call(() -> {
            if (tempChannel != null) {
                tempChannel.close();
            }
            if (tempConn != null) {
                tempConn.close();
            }
        });
    }

    public void sendMessage(byte[] messageBytes, String routingKey) {
        if (messageBytes == null) {
            return;
        }
        if (autoRecoveryChannel == null) {
            tryInitChannel();
        }
        if (autoRecoveryChannel == null) {
            return;
        }
        Channel tempChannel = autoRecoveryChannel;
        if (tempChannel != null) {
            NoThrow
                .call(() -> tempChannel.basicPublish(exchangeName, routingKey, null, messageBytes));
        }
    }
}
