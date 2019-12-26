package me.ele.jarch.athena.worker.manager;

import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.server.pool.ResponseHandler;
import me.ele.jarch.athena.server.pool.ServerSession;

public class ResponseHandlerManager extends Manager {

    private final ResponseHandler handler;
    private final ServerSession session;

    public ResponseHandlerManager(SqlSessionContext ctx, ResponseHandler handler,
        ServerSession session) {
        super(ResponseHandlerManager.class.getName(), ctx);
        this.handler = handler;
        this.session = session;

    }

    @Override protected void manage() {
        handler.sessionAcquired(session);
    }

}
