package me.ele.jarch.athena.server.async;

import me.ele.jarch.athena.allinone.DBConnectionInfo;
import me.ele.jarch.athena.sql.ResultSet;

public class AsyncHeartBeat extends AsyncClient {
    protected long time_to_live;

    protected volatile boolean active = true;
    protected final AsyncResultSetHandler resultHandler;

    protected AsyncHeartBeat(DBConnectionInfo dbConnInfo, AsyncResultSetHandler resultHandler) {
        this(dbConnInfo, resultHandler, 3000);
    }

    protected AsyncHeartBeat(DBConnectionInfo dbConnInfo, AsyncResultSetHandler resultHandler,
        long timeoutMilli) {
        super(dbConnInfo, timeoutMilli);
        this.resultHandler = resultHandler;
        time_to_live = 20 * 1000;
    }

    @Override protected String getQuery() {
        return "SELECT @@global.read_only";
    }

    @Override protected void handleResultSet(ResultSet rs) {
        resultHandler.handleResultSet(rs, true, "sucess");
    }

    @Override protected void handleTimeout() {
        doQuit("timeout, close the channel! " + this.getDbConnInfo().getQualifiedDbId());
    }

    @Override protected synchronized void handleChannelInactive() {
        doQuit("AsyncHeartBeat ChannelInactive at " + this.getStatus() + ", " + this.getDbConnInfo()
            .getQualifiedDbId());
    }

    @Override public synchronized void doQuit(String reason) {
        super.doQuit("");
        if (active) {
            this.active = false;
            this.resultHandler.handleResultSet(null, false, reason);
        }
    }

    /**
     * Normal quit, disconnect the connection to server
     */
    public synchronized void close() {
        this.active = false;
        this.doQuit("");
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    /**
     * @throws IndexOutOfBoundsException will throw if col/row is 0
     */
    public boolean isReadOnly(ResultSet rs) {
        rs.next();
        return rs.getString(1).equals("1");
    }

    public boolean hasSlaveFlag() {
        return false;
    }

    public boolean isOverdue() {
        return System.currentTimeMillis() - getBirthday() > time_to_live;
    }

    public static AsyncHeartBeat newAsyncHeartBeat(DBConnectionInfo dbConnInfo,
        AsyncResultSetHandler handler) {
        switch (dbConnInfo.getDBVendor()) {
            case MYSQL:
                return new MysqlAsyncHeartBeat(dbConnInfo, handler);
            case PG:
                return new PGAsyncHeartBeat(dbConnInfo, handler);
            default:
                return new AsyncHeartBeat(dbConnInfo, handler);
        }
    }

}
