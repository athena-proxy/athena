package me.ele.jarch.athena.worker.manager;

import com.github.mpjct.jmpjct.util.ErrorCode;
import me.ele.jarch.athena.allinone.DBRole;
import me.ele.jarch.athena.constant.Metrics;
import me.ele.jarch.athena.netty.SessionQuitTracer.QuitTrace;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.server.pool.ServerSession;
import me.ele.jarch.athena.server.pool.ServerSession.SERVER_SESSION_STATUS;
import me.ele.jarch.athena.sql.CmdQuery;
import me.ele.jarch.athena.util.etrace.MetricFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerInactiveManager extends Manager {

    private static final Logger logger = LoggerFactory.getLogger(ServerInactiveManager.class);

    private final ServerSession serverSession;

    public ServerInactiveManager(SqlSessionContext ctx, ServerSession serverSession) {
        super(ServerInactiveManager.class.getName(), ctx);
        this.serverSession = serverSession;
    }

    protected void manage() {
        try {
            logger.info("Server channelInactive sqlsessioncontext != null " + this.serverSession);
            MetricFactory.newCounterWithServerSession(Metrics.CONN_SERVER_BROKEN, serverSession)
                .once();
            CmdQuery query = ctx.curCmdQuery;
            boolean isDoubleWrite =
                (serverSession.getRole() == DBRole.GRAY) && (query == null || !query.isGrayRead());

            if (!isDoubleWrite) {
                if (ctx != null && ctx.quitTracer.getRecordedQuitTrace() == QuitTrace.NotQuit) {
                    ctx.quitTracer.reportQuit(QuitTrace.ServerBroken);
                    String message = String
                        .format("db connection close [%s]", serverSession.getActiveQulifiedDbId());
                    ctx.kill(ErrorCode.ABORT_SERVER_BROKEN, message);
                    serverSession.setNeedCloseClient(false);
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            serverSession.setStatus(SERVER_SESSION_STATUS.QUIT);
            serverSession.execute();
        }
    }
}
