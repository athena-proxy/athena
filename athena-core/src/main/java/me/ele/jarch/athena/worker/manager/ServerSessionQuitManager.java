package me.ele.jarch.athena.worker.manager;

import me.ele.jarch.athena.allinone.DBRole;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.server.pool.ServerSession;
import me.ele.jarch.athena.sql.CmdQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerSessionQuitManager extends Manager {

    private static final Logger logger = LoggerFactory.getLogger(ServerSessionQuitManager.class);

    private final ServerSession serverSession;

    public ServerSessionQuitManager(SqlSessionContext ctx, ServerSession serverSession) {
        super(ServerSessionQuitManager.class.getName(), ctx);
        this.serverSession = serverSession;
    }

    protected void manage() {
        try {
            serverSession.closeServerChannelGracefully();
            String ownerInfo = ctx.toString();
            logger.debug(
                String.format("serverSession [%s] is called by doQuit [%s].", this, ownerInfo));
        } finally {
            CmdQuery cmdQuery = ctx.curCmdQuery;
            if (serverSession.getRole().equals(DBRole.GRAY) && (cmdQuery == null || !cmdQuery
                .isGrayRead())) {
                ctx.grayUp.clearGrayUp();
            } else if (serverSession.isNeedCloseClient()) {
                ctx.closeClientChannel();
            }
        }
    }

}
