package me.ele.jarch.athena.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.util.ReferenceCountUtil;
import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.sql.CmdTcpPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * 本类是netty的处理器。它的输入是netty的字节流，输出是MySQL或PG数据包流。 注意，{@code AbstractSqlPacketDecoder} 是有状态的，在netty的多次调用间要保存状态。
 *
 * @author dongyan.xu
 */
public abstract class AbstractSqlPacketDecoder extends MessageToMessageDecoder<ByteBuf> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSqlPacketDecoder.class);
    protected final Queue<byte[]> packets = new ConcurrentLinkedQueue<>();
    protected ByteBuf inputbuf = Unpooled.buffer();
    protected CmdTcpPacket cmdTcpPacket = newPacket();

    protected abstract CmdTcpPacket newPacket();

    @Override public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        ReferenceCountUtil.release(inputbuf);
        super.channelInactive(ctx);
    }

    @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
        throws Exception {
        LOGGER.warn("Unexpected exception from channel :" + ctx.name(), cause);
        ctx.close();
        if (cause instanceof OutOfMemoryError) {
            LOGGER.error("OutOfMemoryError exception from channel :" + ctx.name(), cause);
            System.out.println(String
                .format("OutOfMemoryError exception from channel:%s,exception:%s", ctx.name(),
                    cause.toString()));
            // 延时几秒后退出, 以确保异步日志能够输出到文件中
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(2));
            System.exit(0);
        }
    }

    /*
     * 将字节流转换为MYSQL数据包流。
     *
     * @see io.netty.handler.codec.MessageToMessageDecoder#decode(io.netty.channel.ChannelHandlerContext, java.lang.Object, java.util.List)
     */
    protected abstract void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out)
        throws Exception;

    final protected void extractPackets(ChannelHandlerContext ctx) {
        while (true) {
            try {
                this.cmdTcpPacket.execute();
            } catch (QuitException e) {
                dealExtractPktQuitException(ctx, e);
            }
            if (!this.cmdTcpPacket.isDone()) {
                // 字节不够，等待下一次 netty io
                return;
            }
            this.packets.add(this.cmdTcpPacket.getPacket());
            this.cmdTcpPacket = newPacket();

            // 字节流中可能还有更多的字节，尝试继续处理
        }
    }

    protected void dealExtractPktQuitException(ChannelHandlerContext ctx, QuitException e) {
        LOGGER.info("", e);
    }

    protected Queue<byte[]> clonePackets() {
        Queue<byte[]> queue = new ArrayDeque<>(packets);
        packets.clear();
        return queue;
    }
}
