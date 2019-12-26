package me.ele.jarch.athena.server.pool;

import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.netty.SqlSessionContext;

@FunctionalInterface public interface ServerSessionFactory {
    ServerSession make(ServerSessionPool pool, SqlSessionContext sqlCtx, ResponseHandler handler)
        throws QuitException;
}
