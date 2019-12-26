package me.ele.jarch.athena.sharding.sql;

import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.*;
import me.ele.jarch.athena.sharding.ShardingRouter;
import me.ele.jarch.athena.sql.QUERY_TYPE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ShardingMultiQuery extends ShardingSQL implements SelectForUpdateTrait {
    private static final Logger logger = LoggerFactory.getLogger(ShardingMultiQuery.class);

    private final List<SQLStatement> stmtList;

    public ShardingMultiQuery(SQLObject sqlStmt, List<SQLStatement> stmtList,
        ShardingRouter shardingRouter) {
        super(sqlStmt, shardingRouter);
        this.stmtList = stmtList;
    }

    @Override protected void handleBasicSyntax() {
        queryType = QUERY_TYPE.MULTI_NONE_TRANS;
        for (SQLStatement stmt : stmtList) {
            if (stmt instanceof SQLInsertStatement || stmt instanceof SQLDeleteStatement
                || stmt instanceof SQLUpdateStatement) {
                queryType = QUERY_TYPE.MULTI_TRANS;
            } else if (stmt instanceof SQLSelectStatement) {
                SQLSelect select = ((SQLSelectStatement) stmt).getSelect();
                if (select == null) {
                    continue;
                }
                SQLSelectQuery query = select.getQuery();
                if (!(query instanceof SQLSelectQueryBlock)) {
                    continue;
                }
                if (isSelectForUpdate((SQLSelectQueryBlock) query)) {
                    queryType = QUERY_TYPE.MULTI_TRANS;
                }
            } else if (stmt instanceof SQLCommitStatement || stmt instanceof SQLRollbackStatement) {
                logger.error("the behavior of COMMIT or ROLLBACK in multiquery is uncertain");
            } else if (stmt instanceof SQLSetStatement) {
                logger.error("the behavior of SET in multiquery is uncertain");
            }
        }
        tableName = tableNameAlias = tableNameLowerCase = "multiquery_table";
    }

    @Override protected void handleShardingSyntax() {
    }

    @Override protected void generateSQLs() {
    }
}
