package me.ele.jarch.athena.sharding.sql;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLTruncateStatement;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.sharding.ShardingRouter;
import me.ele.jarch.athena.sharding.ShardingTable;
import me.ele.jarch.athena.sql.QUERY_TYPE;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Created by zhengchao on 16/9/19.
 */
public class ShardingTruncateSQL extends ShardingSQL {
    private String shardingIndex = null;

    public ShardingTruncateSQL(SQLStatement stmt, ShardingRouter shardingRouter) {
        super(stmt, shardingRouter);
    }

    @Override protected void handleBasicSyntax() {
        if (!send2Batch.isBatchAllowed()) {
            return;
        }
        queryType = QUERY_TYPE.TRUNCATE;
        parseTable(((SQLTruncateStatement) sqlStmt).getTableSources().get(0));
    }

    @Override protected void handleShardingSyntax() {
        if (send2Batch.resetAndGetNeedBatchAnalyze()) {
            needSharding = false;
            return;
        }
        shardingIndex = send2Batch.getValueFromComment(Constants.ELE_META_SHARDING_INDEX);
    }

    @Override protected void generateSQLs() {
        if (!needSharding) {
            return;
        }
        // 经过batch分析过的语句，shardingIndex保证一定为整数值。
        List<ShardingTable> shardingTables = new ArrayList<>();
        shardingRouter.getAllShardingTablesOfAllDimensions(tableNameLowerCase).entrySet()
            .forEach(map -> {
                shardingTables.add(map.getValue().get(Integer.valueOf(shardingIndex)));
            });
        results = new TruncateShardingResult(shardingTables);
    }

    public class TruncateShardingResult extends IterableShardingResult {
        private final static String TRUNCATE_SHARDING_TEMPLATE = "truncate table %s";

        public TruncateShardingResult(List<ShardingTable> shardingTables) {
            super(shardingTables);
        }

        @Override public ShardingResult next() {
            if (index >= shardingTables.size()) {
                throw new NoSuchElementException();
            }
            ShardingTable shardingTable = shardingTables.get(index);
            String shardingSQL = String
                .format(TruncateShardingResult.TRUNCATE_SHARDING_TEMPLATE, shardingTable.table);
            ShardingResult r =
                new ShardingResult(shardingSQL, shardingTable.table, shardingTable.database);

            r.shardingRuleIndex = index;
            r.queryType = queryType;
            currentResult = r;

            index++;
            return r;
        }
    }
}
