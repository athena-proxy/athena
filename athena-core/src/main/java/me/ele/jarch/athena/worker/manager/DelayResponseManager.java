package me.ele.jarch.athena.worker.manager;

import com.github.mpjct.jmpjct.util.ErrorCode;
import io.netty.buffer.ByteBuf;
import me.ele.jarch.athena.netty.SessionQuitTracer;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.netty.state.SESSION_STATUS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by rui.wang07 on 2017/9/7.
 */
public class DelayResponseManager extends Manager {
    private static final Logger logger = LoggerFactory.getLogger(DelayResponseManager.class);
    private final ByteBuf byteBuf;

    public DelayResponseManager(SqlSessionContext ctx, ByteBuf byteBuf) {
        super(DelayResponseManager.class.getName(), ctx);
        this.byteBuf = byteBuf;
    }

    @Override protected void manage() {
        if (checkStatus()) {
            ctx.clientWriteAndFlush(byteBuf);
        }
    }

    private boolean checkStatus() {
        if (SESSION_STATUS.QUERY_ANALYZE.equals(ctx.getStatus())) {
            return true;
        }
        logger
            .error("SqlCtx({}) not at state QUERY_ANALYZE:({}) {}", ctx.hashCode(), ctx.getStatus(),
                ctx.getClientInfo());
        String message = "db connection abort";
        try {
            ctx.quitTracer.reportQuit(SessionQuitTracer.QuitTrace.ServerUnsync);
        } finally {
            ctx.kill(ErrorCode.ABORT_SERVER_UNSYNC, message);
        }
        return false;

    }
}
