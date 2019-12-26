package me.ele.jarch.athena.sharding.sql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.SQLUpdateSetItem;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.github.mpjct.jmpjct.util.ErrorCode;
import io.etrace.agent.Trace;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.constant.TraceNames;
import me.ele.jarch.athena.exception.QueryException;
import me.ele.jarch.athena.sharding.ShardingHashCheckLevel;
import me.ele.jarch.athena.sharding.ShardingRouter;
import me.ele.jarch.athena.sharding.ShardingTable;
import me.ele.jarch.athena.sql.QUERY_TYPE;
import me.ele.jarch.athena.util.GreySwitch;
import me.ele.jarch.athena.util.etrace.EtracePatternUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ShardingUpdateSQL extends ShardingSQL {
    private static final Logger logger = LoggerFactory.getLogger(ShardingUpdateSQL.class);

    private SQLUpdateStatement updateStmt;

    public ShardingUpdateSQL(SQLObject sqlStmt, ShardingRouter shardingRouter) {
        super(sqlStmt, shardingRouter);
    }

    @Override protected void handleBasicSyntax() {
        queryType = QUERY_TYPE.UPDATE;
        updateStmt = (SQLUpdateStatement) sqlStmt;
        parseTable(updateStmt.getTableSource());
        checkSingleConditionSQL(updateStmt.getWhere());
    }

    @Override protected void handleShardingSyntax() {
        // 在批量操作的事务中，不支持内嵌update语句
        if (send2Batch.isNeedBatchAnalyze()) {
            throw new QueryException.Builder(ErrorCode.ERR_SQL_SYNTAX_ERROR).setErrorMessage(
                "update sql in transaction of batch operate(like insert,delete) is invalid")
                .bulid();
        }
        if (needSharding) {
            where = updateStmt.getWhere();
            ensureWhereNotNull(where, "no WHERE in UPDATE sql");
            checkIfTryUpdateShardingColunm();
            whereHandler.parseWhere(where);
            parseShardingValues();
        }
    }

    /**
     * 检查update语句中是否会去更新sharding key字段
     */
    private void checkIfTryUpdateShardingColunm() {
        ShardingHashCheckLevel shardingHashCheckLevel =
            GreySwitch.getInstance().getShardingHashCheckLevel();
        if (shardingHashCheckLevel == ShardingHashCheckLevel.PASS) {
            return;
        }
        List<SQLUpdateSetItem> items = updateStmt.getItems();
        for (SQLUpdateSetItem item : items) {
            SQLExpr column = item.getColumn();
            if (!(column instanceof SQLPropertyExpr) && !(column instanceof SQLIdentifierExpr)) {
                logger.warn(String
                    .format("Unrecognized current type，skip this parameter, sql is %s", originSQL));
                continue;
            }
            String columnName;
            if (column instanceof SQLPropertyExpr) {
                columnName = ((SQLPropertyExpr) column).getName();
            } else {
                columnName = ((SQLIdentifierExpr) column).getName();
            }
            if (Objects.isNull(columnName)) {
                logger.warn(String
                    .format("Unrecognized current type，skip this parameter, sql is %s",
                        originSQL.substring(0, 100)));
                continue;
            }
            //如果被更新字段为shardingkey或composedkey对应字段，则报错，同时记录打点。
            if (composedKey.getColumns().contains(columnName) || composedKey.getShardingColumns()
                .contains(columnName)) {
                Map<String, String> tags = new HashMap<>(1);
                tags.put("tableName", tableName);
                String sqlId = EtracePatternUtil.addAndGet(originMostSafeSQL).hash;
                String traceContent = String.format("sqlId : %s, key: %s", sqlId, columnName);
                String traceName = TraceNames.UPDATE_SHARDING_KEY;
                if (composedKey.getColumns().contains(columnName)) {
                    traceName = TraceNames.UPDATE_COMPOSED_KEY;
                }
                Trace.logEvent(traceName, tableName, io.etrace.common.Constants.FAILURE,
                    traceContent, tags);
                break;
            }
        }
    }

    // rewrite the owner in SET expression
    // e.g.
    // UPDATE eleme_order SET eleme_order.attribute_json='abc'
    // ===>
    // UPDATE shd_eleme_order_uid42 SET shd_eleme_order_uid42.attribute_json = 'abc'

    /**
     * @param items        update sql中的set 语句, eg: update table set a = ?
     * @param shardedTable 替换的sharding表名
     * @see UpdateDeleteShardingResult
     */
    public void rewriteOwnerInSETExpr(List<SQLUpdateSetItem> items, String shardedTable) {
        if (Objects.isNull(tableNameAlias)) {
            items.forEach(item -> {
                ShardingUtil.checkLiteralSymbolAndReplaceTable(item.getColumn(), tableNameLowerCase,
                    (p) -> p.setOwner(new SQLIdentifierExpr(shardedTable)), literalSymbol);
            });
        }
    }

    @Override protected IterableShardingResult newShardingResult(List<ShardingTable> shardingTables,
        List<String> shardingIds, int shardingRuleCount) {
        return new UpdateDeleteShardingResult(shardingTables, shardingIds, shardingRuleCount);
    }

    @Override protected void generateSQLs() {
        generateSQLsForUpdateDeleteSelectForUpdate();
    }

    @Override public void generateOriginalShardedSQLs() {
        mostSafeSQL = originMostSafeSQL;
        whiteFields = originWhiteFields;
        generateSQLsForUpdateDeleteSelectForUpdate();
    }

    @Override public void rewriteSQL4ReBalance(String reshardAt) {
        SQLExpr wherExpr = updateStmt.getWhere();
        SQLBinaryOpExpr reshardCondition =
            new SQLBinaryOpExpr(new SQLIdentifierExpr(Constants.CREATED_AT),
                new SQLCharExpr(reshardAt), SQLBinaryOperator.GreaterThan);
        if (Objects.isNull(wherExpr)) {
            updateStmt.setWhere(reshardCondition);
        } else {
            SQLBinaryOpExpr conjunctCondition =
                new SQLBinaryOpExpr(reshardCondition, updateStmt.getWhere(),
                    SQLBinaryOperator.BooleanAnd);
            updateStmt.setWhere(conjunctCondition);
        }

        //只重新进行基本语法解析，由于shardingSQL是延迟生成的,所以无需重新解析sharding规则
        handleSQLConstruct(this, ShardingUtil.debugSql(updateStmt), comment,
            Collections.singletonList(updateStmt));
    }
}
