package me.ele.jarch.athena.server.async;

import me.ele.jarch.athena.sql.ResultSet;

public interface AsyncResultSetHandler {
    public void handleResultSet(ResultSet rs, boolean isDone, String reason);
}
