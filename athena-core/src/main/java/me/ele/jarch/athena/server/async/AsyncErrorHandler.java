package me.ele.jarch.athena.server.async;

/**
 * Created by zhengchao on 16/8/26.
 */
public interface AsyncErrorHandler {
    public enum ERR_TYPE {
        EMPTY, QUERY_FAILED, CONNECT_FAILED, CHANNEL_INACTIVE
    }

    public void handleError(ERR_TYPE reason, String params);
}
