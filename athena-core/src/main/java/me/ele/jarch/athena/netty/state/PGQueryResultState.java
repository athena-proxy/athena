package me.ele.jarch.athena.netty.state;

import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.sql.*;

/**
 * Created by jinghao.wang on 16/11/28.
 */
public class PGQueryResultState extends QueryResultState {
    public PGQueryResultState(SqlSessionContext sqlSessionContext) {
        super(sqlSessionContext);
    }

    @Override protected CmdQueryResult newCmdQueryResult(QueryResultContext ctx) {
        return new PGCmdQueryResult(ctx);
    }

    @Override protected QueryResultContext newDummyQueryResultContext() {
        return new PGDummyQueryResultContext();
    }

    @Override protected QueryResultContext newNonShardingQueryResultContext() {
        return new PGQueryResultContext();
    }

    @Override protected QueryResultContext newInterceptQueryResultContext() {
        throw new IllegalStateException(
            "strange! PostgreSQL protocol dose not support mapping sharding modoe");
    }

    @Override protected ShardingQueryResultContext newShardingQueryResultContext() {
        return new PGShardingQueryResultContext();
    }

    @Override protected boolean hasUnsyncQuery() {
        return super.hasUnsyncQuery() && !sqlSessionContext.clientPackets.isEmpty();
    }

    @Override protected void onQueryResponseSwallowed() {
        sqlSessionContext.setSwallowQueryResponse(false);
        sqlSessionContext.sqlSessionContextUtil.completeTransaction();
    }
}
