package me.ele.jarch.athena.worker.manager;

import com.github.mpjct.jmpjct.util.ErrorCode;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.util.ResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static me.ele.jarch.athena.netty.SessionQuitTracer.QuitTrace;

/**
 * 适用于要Kill的SqlSessionContext不是自身的情况
 */
public class ManualKillerManager extends Manager {
    @SuppressWarnings("unused") private static final Logger logger =
        LoggerFactory.getLogger(ManualKillerManager.class);

    private final QuitTrace quitTrace;
    private final ErrorCode errorCode;
    private final String killMessage;
    private ResponseStatus responseStatus;

    public ManualKillerManager(SqlSessionContext ctx, QuitTrace quitTrace, ErrorCode errorCode,
        String killMessage) {
        super(ManualKillerManager.class.getName(), ctx);
        this.quitTrace = quitTrace;
        this.errorCode = errorCode;
        this.killMessage = killMessage;
    }

    public ManualKillerManager(SqlSessionContext ctx, QuitTrace quitTrace,
        ResponseStatus responseStatus, ErrorCode errorCode, String killMessage) {
        super(ManualKillerManager.class.getName(), ctx);
        this.responseStatus = responseStatus;
        this.quitTrace = quitTrace;
        this.errorCode = errorCode;
        this.killMessage = killMessage;
    }

    private void doKill() {
        if (Objects.isNull(responseStatus)) {
            ctx.quitTracer.reportQuit(quitTrace);
        } else {
            ctx.quitTracer.reportQuit(quitTrace, responseStatus);
        }
        ctx.kill(errorCode, killMessage);
    }

    @Override protected void manage() {
        doKill();
    }

}
