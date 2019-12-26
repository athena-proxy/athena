package me.ele.jarch.athena.sharding.sql;

import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.statement.SQLCommitStatement;
import com.alibaba.druid.sql.ast.statement.SQLRollbackStatement;
import me.ele.jarch.athena.sharding.ShardingRouter;
import me.ele.jarch.athena.sql.QUERY_TYPE;

public class ShardingBatchOtherSQL extends ShardingBatchSQL {

    public ShardingBatchOtherSQL(SQLObject sqlStmt, ShardingRouter shardingRouter) {
        super(sqlStmt, shardingRouter);
    }

    @Override protected void handleBasicSyntax() {
        if (sqlStmt instanceof SQLCommitStatement) {
            queryType = QUERY_TYPE.COMMIT;
        } else if (sqlStmt instanceof SQLRollbackStatement) {
            queryType = QUERY_TYPE.ROLLBACK;
        }
    }

}
