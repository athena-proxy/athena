package me.ele.jarch.athena.worker.manager;

import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.netty.state.SESSION_STATUS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jinghao.wang on 16/4/18.
 */
public class HandshakeManager extends Manager {
    private static final Logger logger = LoggerFactory.getLogger(HandshakeManager.class);

    public HandshakeManager(SqlSessionContext ctx) {
        super(HandshakeManager.class.getName(), ctx);
    }

    @Override protected void manage() {
        try {
            if (checkStatus()) {
                ctx.execute();
            }
        } catch (QuitException e) {
            ctx.doQuit();
            logger.error(
                "HandshakeManager quitException." + ctx.getStatus() + " " + ctx.getClientInfo(), e);
        }
    }

    private boolean checkStatus() {
        return ctx.getStatus() == SESSION_STATUS.CLIENT_FAKE_HANDSHAKE;
    }
}
