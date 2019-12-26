package me.ele.jarch.athena.worker.manager;

import me.ele.jarch.athena.constant.Metrics;
import me.ele.jarch.athena.netty.SessionQuitTracer;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.scheduler.DBChannelDispatcher;
import me.ele.jarch.athena.scheduler.HangSessionMonitor;
import me.ele.jarch.athena.util.NoThrow;
import me.ele.jarch.athena.util.etrace.MetricFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientInactiveManager extends Manager {

    private static final Logger logger = LoggerFactory.getLogger(ClientInactiveManager.class);

    public ClientInactiveManager(SqlSessionContext ctx) {
        super(ClientInactiveManager.class.getName(), ctx);
    }

    protected void manage() {
        try {
            if (ctx.authenticator.isDalHealthCheckUser) {
                // do not log, keeping silent for health check heartbeat connections.
                assert ctx.getHolder() == null; // A heartbeat connection binds no holders.
                return;
            }

            logger.info("client channelInactive: " + ctx.getClientInfo());
            if (ctx.sqlSessionContextUtil.secondCommitId % 2 == 0x1) {
                MetricFactory.newCounterWithSqlSessionContext(Metrics.SOMEONE_COMMIT_FAIL, ctx)
                    .once();
                String errLog = "someone commit failure," + ctx + ", Query :";
                if (ctx.curCmdQuery != null) {
                    errLog = errLog + ctx.curCmdQuery.query;
                }
                logger.error(errLog);
            }
            DBChannelDispatcher holder = ctx.getHolder();
            if (holder != null) {
                holder.cCnt.decrementAndGet();
            }
            ctx.quitTracer.reportQuit(SessionQuitTracer.QuitTrace.ClientBroken);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            NoThrow.call(() -> HangSessionMonitor.getInst().removeCtx(ctx));
            ctx.doQuit();
        }
    }
}
