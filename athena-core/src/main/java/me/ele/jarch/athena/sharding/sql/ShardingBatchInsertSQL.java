package me.ele.jarch.athena.sharding.sql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement.ValuesClause;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import me.ele.jarch.athena.sharding.ShardingRouter;
import me.ele.jarch.athena.sharding.ShardingTable;
import me.ele.jarch.athena.sql.QUERY_TYPE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zhengchao on 16/8/30.
 */
public class ShardingBatchInsertSQL extends ShardingBatchSQL {
    private static final Logger logger = LoggerFactory.getLogger(ShardingBatchInsertSQL.class);
    private MySqlInsertStatement mysqlStmt;
    private final Map<String, Integer> composeColumnNameWithIndex = new HashMap<>(2);

    public ShardingBatchInsertSQL(SQLObject sqlStmt, ShardingRouter shardingRouter) {
        super(sqlStmt, shardingRouter);
    }

    @Override protected void handleBasicSyntax() {
        queryType = QUERY_TYPE.INSERT;
        mysqlStmt = (MySqlInsertStatement) sqlStmt;
        parseTable(mysqlStmt.getTableSource());
    }

    @Override protected void handleShardingSyntax() {
        if (!needSharding) {
            return;
        }
        parseColumns(mysqlStmt.getColumns());
    }

    private void parseColumns(List<SQLExpr> list) {
        for (int i = 0; i < list.size(); i++) {
            String column = ShardingUtil.removeLiteralSymbol(list.get(i).toString(), literalSymbol);
            if (composedKey.getColumns().contains(column)) {
                composeColumnNameWithIndex.put(column, i);
            }
        }
    }

    @Override protected void generateSQLs() {
        if (!needSharding) {
            super.generateSQLs();
            return;
        }
        Map<String, List<ValuesClause>> tableValuesClauseMap = new HashMap<>();
        logger.info("{} has {} values", batchTransLogIdStr, mysqlStmt.getValuesList().size());
        for (ValuesClause vc : mysqlStmt.getValuesList()) {
            shuffleValuesClause(vc, tableValuesClauseMap);
        }
        makeShardingBatchQuery(tableValuesClauseMap);
    }

    private void shuffleValuesClause(ValuesClause vc, Map<String, List<ValuesClause>> tableIdMap) {
        Map.Entry<String, Integer> entry = composeColumnNameWithIndex.entrySet().iterator().next();
        final int composeKeyColumnIndex = entry.getValue();
        final String composeKeyName = entry.getKey();
        String id = ShardingUtil.getVal(vc.getValues().get(composeKeyColumnIndex));
        // 只解析一列，用于防止多维sharding时，一张分表中重复插入同一个ValuesClause
        String column = composedKey.getShardingColumns().get(0);
        ShardingTable t =
            shardingRouter.getShardingTableByComposedKey(tableNameLowerCase, column, id);
        ensureShardingTableNotNull(t,
            () -> "ShardingTable of composed key == null of shardingColumn = " + composeKeyName
                + " and id = " + id);
        List<ValuesClause> targetVCList =
            tableIdMap.computeIfAbsent(t.table, (table) -> new ArrayList<>());
        if (!targetVCList.contains(vc)) {
            targetVCList.add(vc);
        }
    }

    private void makeShardingBatchQuery(Map<String, List<ValuesClause>> tableValuesClauseMap) {
        tableValuesClauseMap.forEach((shardingTable, valuesClauses) -> {
            // 由于新版本移除了`setValuesList`方法,所以采用先清空原有valuesList,后添加ValuesClause的方法来实现原有功能
            mysqlStmt.getValuesList().clear();
            valuesClauses.forEach(mysqlStmt::addValueCause);
            resultShardingQuerys
                .add(generateQuery(shardingTable, ShardingUtil.debugSql(mysqlStmt)));
        });
    }
}
