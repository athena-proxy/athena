package me.ele.jarch.athena.util.health.check;

import me.ele.jarch.athena.allinone.DBConnectionInfo;
import me.ele.jarch.athena.server.async.AsyncHeartBeat;
import me.ele.jarch.athena.server.async.AsyncResultSetHandler;

public class MysqlHealthCheckClient extends AsyncHeartBeat {

    private volatile String sql = "";

    protected MysqlHealthCheckClient(DBConnectionInfo dbConnInfo,
        AsyncResultSetHandler resultHandler, long timeoutMilli) {
        super(dbConnInfo, resultHandler, timeoutMilli);
    }

    /**
     * send sqls to perform health check
     * 1) SELECT 1 (main db)
     * 2) SELECT 1 from shard_table where 1=0; (sharding db)
     */
    @Override protected String getQuery() {
        return this.sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getSql() {
        return sql;
    }
}

