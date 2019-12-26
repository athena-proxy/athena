package me.ele.jarch.athena.sharding.sql;

import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.statement.SQLTruncateStatement;
import me.ele.jarch.athena.sharding.ShardingRouter;
import me.ele.jarch.athena.sharding.ShardingTable;
import me.ele.jarch.athena.sql.BatchQuery;
import me.ele.jarch.athena.sql.QUERY_TYPE;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zhengchao on 16/9/20.
 */
public class ShardingBatchTruncateSQL extends ShardingBatchSQL {
    public ShardingBatchTruncateSQL(SQLObject sqlStmt, ShardingRouter shardingRouter) {
        super(sqlStmt, shardingRouter);
    }

    @Override protected void handleBasicSyntax() {
        queryType = QUERY_TYPE.TRUNCATE;
        parseTable(((SQLTruncateStatement) sqlStmt).getTableSources().get(0));
    }

    @Override protected void generateSQLs() {
        if (!needSharding) {
            super.generateSQLs();
            return;
        }

        List<ShardingTable> shardingTableList = shardingRouter.getAllShardingTable(tableName);
        final AtomicInteger index = new AtomicInteger(0);
        shardingTableList.forEach(shardingTable -> {
            BatchQuery query = generateQuery(shardingTable.table, ShardingUtil.debugSql(sqlStmt));
            query.setShardingIndex(index.getAndIncrement());
            resultShardingQuerys.add(query);
        });
    }
}
