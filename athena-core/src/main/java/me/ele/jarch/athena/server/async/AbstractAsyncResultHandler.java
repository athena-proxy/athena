package me.ele.jarch.athena.server.async;

import me.ele.jarch.athena.sql.ResultSet;

public abstract class AbstractAsyncResultHandler implements AsyncResultSetHandler {
    private AbstractAsyncErrorHandler errorHandler;

    public void setErrorHandler(AbstractAsyncErrorHandler errorHandler) {
        this.errorHandler = errorHandler;
    }

    @Override public void handleResultSet(ResultSet rs, boolean isDone, String params) {
        if (!rs.errorHappened()) {
            handleResultSet(rs, params);
            return;
        }
        if (errorHandler != null) {
            errorHandler.handleError(AsyncErrorHandler.ERR_TYPE.QUERY_FAILED, rs.getErr(), params);
        }
    }

    public abstract void handleResultSet(ResultSet rs, String params);
}
