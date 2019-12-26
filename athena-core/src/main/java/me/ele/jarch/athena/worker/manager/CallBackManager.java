package me.ele.jarch.athena.worker.manager;

import io.netty.util.concurrent.Future;
import me.ele.jarch.athena.netty.SessionQuitTracer;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.server.pool.ServerSession;
import me.ele.jarch.athena.server.pool.ServerSessionErrorHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CallBackManager extends Manager {

    private static final Logger logger = LoggerFactory.getLogger(CallBackManager.class);


    public enum CallBackStatus {
        SERVER_FLUSH, CLIENT_FLUSH_QUIT, CLIENT_FLUSH, CLIENT_LAZY_WRITE
    }


    private final Future<? super Void> future;
    private final CallBackStatus status;
    private ServerSession serverSession;

    public CallBackManager setServerSession(ServerSession serverSession) {
        this.serverSession = serverSession;
        return this;
    }

    public CallBackManager(SqlSessionContext ctx, Future<? super Void> future,
        CallBackStatus status) {
        super(CallBackManager.class.getName() + "." + status.toString(), ctx);
        this.future = future;
        this.status = status;
    }

    protected void manage() {
        switch (status) {
            case SERVER_FLUSH:
                doServerFlush();
                break;
            case CLIENT_FLUSH_QUIT:
                doClientFlushQuit();
                break;
            case CLIENT_FLUSH:
                doClientFlush();
                break;
            case CLIENT_LAZY_WRITE:
                doLazyWrite();
                break;
            default:
                defaultPrint();
        }
    }

    // ------------SERVER_FLUSH---------------------------
    ServerSessionErrorHandler handler;

    public CallBackManager setErrorHandler(ServerSessionErrorHandler handler) {
        this.handler = handler;
        return this;
    }

    private void doServerFlush() {
        if (!future.isSuccess() && handler != null) {
            handler.invoke(serverSession);
        }
    }

    // ------------SERVER_FLUSH---------------------------

    // ------------CLIENT_FLUSH_QUIT---------------------------
    private void doClientFlushQuit() {
        try {
            if (!future.isSuccess()) {
                logger.error("Failed to send quit packets to client " + ctx);
            }
        } finally {
            ctx.doQuit();
        }
    }

    // ------------CLIENT_FLUSH_QUIT---------------------------

    // ------------CLIENT_FLUSH---------------------------
    private void doClientFlush() {
        if (!future.isSuccess()) {
            try {
                ctx.quitTracer.reportQuit(SessionQuitTracer.QuitTrace.Send2ClientBroken);
            } finally {
                ctx.doQuit();
            }
        }
    }

    // ------------CLIENT_FLUSH---------------------------

    // ------------CLIENT_LAZY_WRITE---------------------------
    private boolean isClientLazyWriteHasFailed = false;

    public CallBackManager setClientLazyWriteHasFailed(boolean hasFailed) {
        this.isClientLazyWriteHasFailed = hasFailed;
        return this;
    }

    private void doLazyWrite() {
        if (!future.isSuccess()) {
            if (!isClientLazyWriteHasFailed) {
                try {
                    isClientLazyWriteHasFailed = true;
                    ctx.quitTracer.reportQuit(SessionQuitTracer.QuitTrace.Send2ClientBroken);
                } finally {
                    ctx.doQuit();
                }
            }
        }
    }

    // ------------CLIENT_LAZY_WRITE---------------------------

    private void defaultPrint() {
        logger.error("Unknown status:" + status);
    }
}
