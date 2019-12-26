package me.ele.jarch.athena.server.async;

import me.ele.jarch.athena.allinone.DBConnectionInfo;
import me.ele.jarch.athena.sql.ResultSet;

/**
 * 用完即弃的一次性连接,用于执行一次简单的到数据的查询。
 * Created by jinghao.wang on 16/11/9.
 */
public class AsyncOneTimeClient extends AsyncClient {
    private final String query;
    private final AsyncResultSetHandler resultHandler;
    private volatile boolean autoQuit = false;

    public AsyncOneTimeClient(DBConnectionInfo dbConnInfo, String query,
        AsyncResultSetHandler resultHandler) {
        super(dbConnInfo);
        this.query = query;
        this.resultHandler = resultHandler;
    }

    @Override protected String getQuery() {
        return query;
    }

    @Override protected void handleResultSet(ResultSet resultSet) {
        resultHandler.handleResultSet(resultSet, true, "sucess");
        autoQuit = true;
        super.doQuit("");
    }

    @Override protected void handleTimeout() {
        String quitReason =
            "timeout, close the channel! " + this.getDbConnInfo().getQualifiedDbId();
        this.resultHandler.handleResultSet(null, false, quitReason);
        super.doQuit(quitReason);
    }

    @Override protected void handleChannelInactive() {
        String quitReason =
            "AsyncOneTimeClient ChannelInactive at " + this.getStatus() + ", " + this
                .getDbConnInfo().getQualifiedDbId();
        if (!autoQuit) {
            this.resultHandler.handleResultSet(null, false, quitReason);
            super.doQuit(quitReason);
        }
    }
}
