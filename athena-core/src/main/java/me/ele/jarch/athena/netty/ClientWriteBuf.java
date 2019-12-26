package me.ele.jarch.athena.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;

import java.util.LinkedList;
import java.util.List;

public class ClientWriteBuf {
    private final List<ByteBuf> bufs = new LinkedList<ByteBuf>();
    private int bufSize;

    public ClientWriteBuf() {
    }

    /**
     * @param buf
     * @return sum of the buf readableBytes
     */
    public int write(ByteBuf buf) {
        bufs.add(buf);
        bufSize += buf.readableBytes();
        return bufSize;
    }

    public ByteBuf readall() {
        try {
            if (this.bufs.isEmpty()) {
                return Unpooled.EMPTY_BUFFER;
            }
            if (this.bufs.size() == 1) {
                return bufs.get(0);
            }
            return new CompositeByteBuf(UnpooledByteBufAllocator.DEFAULT, false, bufs.size(), bufs);
        } finally {
            bufSize = 0;
            bufs.clear();
        }
    }

    public int getBufSize() {
        return bufSize;
    }

}
