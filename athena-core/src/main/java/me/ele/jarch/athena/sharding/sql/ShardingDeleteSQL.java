package me.ele.jarch.athena.sharding.sql;

import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.github.mpjct.jmpjct.util.ErrorCode;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.exception.QueryException;
import me.ele.jarch.athena.sharding.ShardingRouter;
import me.ele.jarch.athena.sharding.ShardingTable;
import me.ele.jarch.athena.sql.QUERY_TYPE;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;

public class ShardingDeleteSQL extends ShardingSQL {
    private SQLDeleteStatement deleteStmt;

    public ShardingDeleteSQL(SQLObject sqlStmt, ShardingRouter shardingRouter) {
        super(sqlStmt, shardingRouter);
    }

    @Override protected void handleBasicSyntax() {
        queryType = QUERY_TYPE.DELETE;
        deleteStmt = (SQLDeleteStatement) sqlStmt;
        parseTable(deleteStmt.getTableSource());
        checkSingleConditionSQL(deleteStmt.getWhere());
    }

    @Override protected void handleShardingSyntax() {
        if (needSharding) {
            where = deleteStmt.getWhere();
            // 在未经过batch分析时，如果解析出来需要sharding，但是where为空，直接截断，不再发送到batch。
            ensureWhereNotNull(where, "no WHERE in DELETE sql");
            whereHandler.parseWhere(where);
            parseShardingValues();
        }
        if (send2Batch.resetAndGetNeedBatchAnalyze()) {
            // 只有在表是sharding表的情况下才强制保证有sharding相关条件
            if (needSharding) {
                ensureHasComposeKeyValue4Batch();
            }
            // 在即将发往batch分析时，不再对delete做sharding分析
            needSharding = false;
        }
    }

    private void ensureHasComposeKeyValue4Batch() {
        if (validShardingValues.composeColumnNameWithValues.isEmpty()
            && whereHandler.shardingIndex == -1) {
            throw new QueryException.Builder(ErrorCode.ERR_NO_COMPOSED_ID).setErrorMessage(
                "this sql need to be batch sharded but cannot extract composed key: " + composedKey
                    .getColumn()).bulid();
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

    @Override protected void generateSQLsForUpdateDeleteSelectForUpdate() {
        if (!needSharding) {
            return;
        }
        if (!send2Batch.isShuffledByBatchContext()) {
            // batch开关置为off时，会走此逻辑
            super.generateSQLsForUpdateDeleteSelectForUpdate();
            return;
        }
        // batch功能开启,且batch已经分析过
        String shardColVal = send2Batch.getValueFromComment(Constants.ELE_META_SHARD_COL_VAL);
        if (StringUtils.isNotEmpty(shardColVal)) {
            // batch 情况下,经过分析的sql,以注释中带的composeKeyValues为准,忽略从SQL中解析的值
            validShardingValues.composeColumnNameWithValues.clear();
            //多composeKey的情况下,只要对composeKeyValue应用正确的规则即可,无需关心名字和值是否对应
            validShardingValues.composeColumnNameWithValues
                .put(composedKey.getColumns().get(0), Collections.singleton(shardColVal));
        }
        super.generateSQLsForUpdateDeleteSelectForUpdate();
    }

}
