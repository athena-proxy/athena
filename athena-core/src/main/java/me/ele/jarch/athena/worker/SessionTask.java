package me.ele.jarch.athena.worker;

import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.netty.state.SESSION_STATUS;
import me.ele.jarch.athena.util.AthenaConfig;
import me.ele.jarch.athena.util.GrayType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SessionTask extends Task {

    private static final Logger LOGGER = LoggerFactory.getLogger(SessionTask.class);

    private final SESSION_STATUS status;

    public SessionTask(String name, SqlSessionContext ctx, TaskPriority priority) {
        super(name, ctx, priority);
        this.status = ctx.getStatus();
    }

    public void execute() {
        if (this.status != ctx.getStatus()) {
            if (ctx.allowUnsyncQuery()) {
                return;
            }
            if (AthenaConfig.getInstance().getGrayType() == GrayType.GRAY
                && this.status == SESSION_STATUS.GRAY_QUERY_HANDLE) {
                LOGGER.warn(String
                    .format("Inconsistent state,JobStatus:%s,contextStatus:%s,task:%s,context:%s",
                        this.status, ctx.getStatus(), toString(), ctx));
                return;
            }
            LOGGER.error(String
                .format("Inconsistent state,JobStatus:%s,contextStatus:%s,task:%s,context:%s",
                    this.status, ctx.getStatus(), toString(), ctx));
            ctx.doQuit();
            return;
        }
        try {
            ctx.execute();
        } catch (Exception t) {
            LOGGER.error(String
                .format("JobStatus:%s,contextStatus:%s,task:%s", this.status, ctx.getStatus(),
                    toString()), t);
            ctx.doQuit();
        }
    }

}
