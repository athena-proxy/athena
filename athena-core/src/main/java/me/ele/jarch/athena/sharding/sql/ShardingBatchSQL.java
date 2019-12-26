package me.ele.jarch.athena.sharding.sql;

import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLTruncateStatement;
import me.ele.jarch.athena.sharding.ShardingRouter;
import me.ele.jarch.athena.sql.BatchQuery;

import java.util.LinkedList;
import java.util.List;

public abstract class ShardingBatchSQL extends ShardingSQL {
    public String batchTransLogIdStr = "";
    protected List<BatchQuery> resultShardingQuerys = new LinkedList<>();
    protected final String BATCH_OTHER_TABLE = "batch_other_table";

    public ShardingBatchSQL(SQLObject sqlStmt, ShardingRouter shardingRouter) {
        super(sqlStmt, shardingRouter);
    }

    public List<BatchQuery> getResultShardingQuerys() {
        return resultShardingQuerys;
    }

    @Override protected void handleShardingSyntax() {

    }

    @Override protected void generateSQLs() {
        resultShardingQuerys.add(generateQuery(BATCH_OTHER_TABLE, ShardingUtil.debugSql(sqlStmt)));
    }

    private static ShardingBatchSQL createShardingSQL(List<SQLStatement> stmtList,
        ShardingRouter shardingRouter) {
        SQLStatement stmt = stmtList.get(0);
        if (stmt instanceof SQLInsertStatement) {
            return new ShardingBatchInsertSQL(stmt, shardingRouter);
        } else if (stmt instanceof SQLTruncateStatement) {
            return new ShardingBatchTruncateSQL(stmt, shardingRouter);
        } else if (stmt instanceof SQLDeleteStatement) {
            return new ShardingBatchDeleteSQL(stmt, shardingRouter);
        } else {
            return new ShardingBatchOtherSQL(stmt, shardingRouter);
        }
    }

    public static ShardingBatchSQL handleBatchSQLBasicly(String sql, String queryComment,
        ShardingRouter shardingRouter) {
        // DAL will never send invalid sql to batch,so we don't need catch exception here.
        List<SQLStatement> stmtList = parseMySQLStmt(sql);
        ShardingBatchSQL shardingSQL = createShardingSQL(stmtList, shardingRouter);
        handleSQLConstruct(shardingSQL, sql, queryComment, stmtList);
        return shardingSQL;
    }

    protected BatchQuery generateQuery(String shardTable, String querySql) {
        BatchQuery query = new BatchQuery();
        query.setShardTable(shardTable);
        query.setQueryWithoutComment(querySql);
        query.setType(queryType);
        return query;
    }
}
