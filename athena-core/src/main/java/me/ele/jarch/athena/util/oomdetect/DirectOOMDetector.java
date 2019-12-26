package me.ele.jarch.athena.util.oomdetect;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.internal.OutOfDirectMemoryError;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.netty.AthenaServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Created by shaoyang.qi on 2017/9/26.
 * 此类用于检测堆外内存溢出，如果检测到则关闭DAL进程以重启
 */
public class DirectOOMDetector {
    private static final Logger LOGGER = LoggerFactory.getLogger(DirectOOMDetector.class);
    private static final long DETECT_INTERVAL = TimeUnit.SECONDS.toMillis(5);

    private DirectOOMDetector() {
    }

    private static class INNER {
        private static final DirectOOMDetector INSTANCE = new DirectOOMDetector();
    }

    public static DirectOOMDetector getInstance() {
        return INNER.INSTANCE;
    }

    public void addDetectJob() {
        AthenaServer.commonJobScheduler.addJob("direct_oom_detector", DETECT_INTERVAL, () -> run());
    }

    private void run() {
        ByteBuf byteBuf = null;
        try {
            byteBuf = Unpooled.directBuffer(Constants.MAX_AUTOREAD_TRIGGER_SIZE);
        } catch (Throwable throwable) {
            checkThrowable(throwable);
        } finally {
            if (Objects.nonNull(byteBuf)) {
                ReferenceCountUtil.release(byteBuf);
            }
        }
    }

    private void checkThrowable(Throwable throwable) {
        OutOfDirectMemoryError error = tryDecodeDirectOOMError(throwable);
        if (Objects.isNull(error)) {
            return;
        }
        LOGGER.error("detect OutOfDirectMemoryError exception in DirectOOMDetector.", throwable);
        System.out.println(String
            .format("detect OutOfDirectMemoryError exception in DirectOOMDetector.exception:%s",
                error.toString()));
        // 延时几秒后退出, 以确保异步日志能够输出到文件中
        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(2));
        System.exit(0);
    }

    private OutOfDirectMemoryError tryDecodeDirectOOMError(Throwable throwable) {
        if (Objects.isNull(throwable)) {
            return null;
        }
        if (throwable instanceof OutOfDirectMemoryError) {
            return (OutOfDirectMemoryError) throwable;
        }
        return tryDecodeDirectOOMError(throwable.getCause());
    }
}
