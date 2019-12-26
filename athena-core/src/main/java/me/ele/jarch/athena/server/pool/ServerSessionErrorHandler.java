package me.ele.jarch.athena.server.pool;

@FunctionalInterface public interface ServerSessionErrorHandler {
    void invoke(ServerSession serverSession);
}
