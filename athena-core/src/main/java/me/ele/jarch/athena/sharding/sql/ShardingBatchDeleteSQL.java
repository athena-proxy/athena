package me.ele.jarch.athena.sharding.sql;

import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import me.ele.jarch.athena.sharding.ShardingRouter;
import me.ele.jarch.athena.sharding.ShardingTable;
import me.ele.jarch.athena.sql.BatchQuery;
import me.ele.jarch.athena.sql.QUERY_TYPE;

import java.util.*;

public class ShardingBatchDeleteSQL extends ShardingBatchSQL {
    private SQLDeleteStatement mysqlStmt;

    public ShardingBatchDeleteSQL(SQLObject sqlStmt, ShardingRouter shardingRouter) {
        super(sqlStmt, shardingRouter);
    }

    @Override protected void handleBasicSyntax() {
        queryType = QUERY_TYPE.DELETE;
        mysqlStmt = (SQLDeleteStatement) sqlStmt;
        parseTable(mysqlStmt.getTableSource());
        checkSingleConditionSQL(mysqlStmt.getWhere());
    }

    @Override protected void handleShardingSyntax() {
        if (!needSharding) {
            return;
        }
        //DAL will never send sql without where clause to batch here
        where = mysqlStmt.getWhere();
        whereHandler.parseWhere(where);
        parseShardingValues();
    }

    @Override protected void generateSQLs() {
        if (!needSharding) {
            super.generateSQLs();
            return;
        }
        Map<String, List<String>> tableIdsMap = new HashMap<>();
        Map.Entry<String, Set<String>> entry =
            validShardingValues.composeColumnNameWithValues.entrySet().iterator().next();
        final Set<String> composeKeyValues = entry.getValue();
        final String composeKeyName = entry.getKey();
        for (String id : composeKeyValues) {
            shuffleIds(composeKeyName, id, tableIdsMap);
        }
        makeShardingBatchQuery(tableIdsMap);
    }

    private void shardingToTable(String composeKeyName, String id, List<ShardingTable> tables,
        List<String> shardingKeys) {
        composedKey.getShardingColumns().forEach((column) -> {
            ShardingTable t =
                shardingRouter.getShardingTableByComposedKey(tableNameLowerCase, column, id);
            ensureShardingTableNotNull(t,
                () -> "ShardingTable of composed key == null of shardingColumn = " + composeKeyName
                    + " and id = " + id);
            if (!tables.stream().filter(e -> e.table.equals(t.table)).findAny().isPresent()) {
                tables.add(t);
                shardingKeys.add(column);
            }
        });
    }

    private void shuffleIds(String composeKeyName, String id,
        Map<String, List<String>> tableIdsMap) {
        List<ShardingTable> shardTables = new ArrayList<>();
        shardingToTable(composeKeyName, id, shardTables, new ArrayList<>());
        if (shardTables.isEmpty()) {
            return;
        }
        ShardingTable shardTable = shardTables.get(0);
        List<String> ids =
            tableIdsMap.computeIfAbsent(shardTable.table, (table) -> new ArrayList<>());
        if (!ids.contains(id)) {
            ids.add(id);
        }
    }

    private void makeShardingBatchQuery(Map<String, List<String>> tableIdsMap) {
        String sql = ShardingUtil.debugSql(mysqlStmt);
        tableIdsMap.forEach((shardingTable, values) -> {
            BatchQuery query = generateQuery(shardingTable, sql);
            query.setShardColValue(values.get(0));
            resultShardingQuerys.add(query);
        });
    }

}
