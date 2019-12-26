package me.ele.jarch.athena.server.pool;

public abstract class ResponseHandler {
    /**
     * response for connectioned
     */
    public void sessionAcquired(ServerSession session) {
    }

    /**
     * response for error
     */
    public void sessionError(Throwable t, ServerSession session) {
    }

    /**
     * other response
     */

}
