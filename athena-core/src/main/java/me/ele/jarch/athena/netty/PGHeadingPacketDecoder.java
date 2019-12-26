package me.ele.jarch.athena.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import me.ele.jarch.athena.pg.proto.*;
import me.ele.jarch.athena.util.PacketLengthUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 该类用于PG起始连接时的协议预处理
 * <p>
 * 在PG协议中,起始的SSL,StartupMessage和cancel请求并没有带type类型,所以它们的包结构和其他PG包结构有所不同
 * 为了保持解码的一致性,所以在处理常规PG协议前,先使用该类进行预处理
 * 人为添加自定义的type类型前缀(生成dal内部虚拟的数据结构,其编解码采用loadFromVirtualPacket)
 * <p>
 * 且SSL请求发送后,下一次请求会继续发送StartupMessage,所以在处理SSL请求后,仍需此类继续处理StartupMessage
 *
 * @see me.ele.jarch.athena.pg.proto.PGFlags
 */
public class PGHeadingPacketDecoder extends ByteToMessageDecoder {
    private static final Logger LOGGER = LoggerFactory.getLogger(PGHeadingPacketDecoder.class);

    private int length = -1;
    // 不带length的协议payload
    private byte[] payload = null;

    @Override protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf,
        List<Object> list) throws Exception {
        if (length == -1 && byteBuf.readableBytes() < 4) {
            return;
        }
        if (length == -1) {
            length = byteBuf.readInt();
            PacketLengthUtil.checkValidLength(length);
            payload = new byte[length - 4];
        }
        if (!PacketLengthUtil.isValidPayload(payload)) {
            // 如果length不为-1但是payload为null，说明length过大，不再处理byteBuf
            byteBuf.clear();
            return;
        }
        if (byteBuf.readableBytes() < length - 4) {
            return;
        }
        byteBuf.readBytes(payload);

        // 判断是否是SSL尝试
        if (length == SSLRequestMessage.SSL_REQUEST_LENGTH
            && payload.length == SSLRequestMessage.SSL_REQUEST_LENGTH - 4
            && new PGProto(payload).readInt32() == SSLRequestMessage.SSL_REQUEST_NUMBER) {
            // 如果是SSL尝试,那么给后端的decoder继续处理,同时length赋值-1,以待处理后续的非常规请求
            LOGGER.debug("SSL Request");
            list.add(Unpooled.buffer().writeByte(PGFlags.C_SSL_REQUEST).writeInt(length)
                .writeBytes(payload));
            length = -1;
            return;
        }
        if (length == CancelRequest.CANCEL_REQUEST_LENGTH
            && payload.length == CancelRequest.CANCEL_REQUEST_LENGTH - 4
            && new PGProto(payload).readInt32() == CancelRequest.CANCEL_REQUEST_CODE) {
            list.add(Unpooled.buffer().writeByte(PGFlags.C_CANCEL_REQUEST).writeInt(length)
                .writeBytes(payload));
            length = -1;
            return;
        }
        // 判断是否是StartupMessage
        if (new PGProto(payload).readInt32() == StartupMessage.PROTOCOL_VERSION_NUM) {
            LOGGER.debug("StartupMessage Request");
            list.add(Unpooled.buffer().writeByte(PGFlags.C_STARTUP_MESSAGE).writeInt(length)
                .writeBytes(payload));
            // 接受到StartupMessage之后,就不再需要此类了
            channelHandlerContext.pipeline().remove(this);
        }
    }
}
