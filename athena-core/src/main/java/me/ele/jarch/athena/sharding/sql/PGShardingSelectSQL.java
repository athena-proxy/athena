package me.ele.jarch.athena.sharding.sql;

import com.alibaba.druid.sql.ast.SQLObject;
import me.ele.jarch.athena.sharding.ShardingRouter;

public class PGShardingSelectSQL extends ShardingSelectSQL {
    public PGShardingSelectSQL(SQLObject stmt, ShardingRouter shardingRouter) {
        super(stmt, shardingRouter);
    }

    /**
     * do nothing for PostgreSQL parse other
     */
    @Override protected void parseOtherBasicSyntax() {
        //NOOP
    }
}
