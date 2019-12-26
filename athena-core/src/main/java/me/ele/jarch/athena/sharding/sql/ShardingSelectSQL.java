package me.ele.jarch.athena.sharding.sql;

import com.alibaba.druid.sql.ast.*;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.sharding.ShardingConfig;
import me.ele.jarch.athena.sharding.ShardingRouter;
import me.ele.jarch.athena.sharding.ShardingTable;
import me.ele.jarch.athena.sql.QUERY_TYPE;
import me.ele.jarch.athena.sql.seqs.GeneratorUtil;
import me.ele.jarch.athena.util.AggregateFunc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ShardingSelectSQL extends ShardingSQL implements SelectForUpdateTrait {
    private static final Logger logger = LoggerFactory.getLogger(ShardingSelectSQL.class);

    protected boolean isLocked;

    protected int limit = -1;

    protected int offset = -1;

    private List<SQLPropertyExpr> needRewriteOwnerInSelectedColuomns = new ArrayList<>();

    private List<AggregateFunc> aggrMethods = new ArrayList<>();

    private List<String> groupByItems = new ArrayList<>();

    private SQLSelectQueryBlock query;

    protected final ShardingInContidon inContidon = new ShardingInContidon();

    public ShardingSelectSQL(SQLObject stmt, ShardingRouter shardingRouter) {
        super(((SQLSelectStatement) stmt).getSelect().getQuery(), shardingRouter);
    }

    @Override protected void handleBasicSyntax() {
        queryType = QUERY_TYPE.SELECT;
        if (!(sqlStmt instanceof SQLSelectQueryBlock)) {
            needSharding = false;
            return;
        }
        query = (SQLSelectQueryBlock) sqlStmt;
        isLocked = isSelectForUpdate(query);
        if (isLocked) {
            queryType = QUERY_TYPE.SELECT_FOR_UPDATE;
        }
        checkBindMasterExpr(query.getWhere());
        checkSingleConditionSQL(query.getWhere());
        parseTable(query.getFrom());
        handleLimit(query.getLimit());
        handleOrderBy(query.getOrderBy());
        handleGroupBy(query.getGroupBy());
        parseOtherBasicSyntax();
    }

    protected void parseOtherBasicSyntax() {
        parseQueryAutoCommitSQL();
        //查询系统变量分析必须在queryAutoCommit分析之后，否则会覆盖autoCommit的分析
        parseQuerySystemVariables();
    }

    private void parseQuerySystemVariables() {
        if (Objects.nonNull(query.getFrom())) {
            return;
        }
        List<SQLSelectItem> selectItems = query.getSelectList();
        if (selectItems.isEmpty()) {
            return;
        }
        SQLSelectItem selectItem = selectItems.iterator().next();
        SQLExpr parentSqlExpr = selectItem.getExpr();
        SQLExpr sqlExpr = parentSqlExpr;
        if (parentSqlExpr instanceof SQLPropertyExpr) {
            SQLPropertyExpr propertyExpr = (SQLPropertyExpr) parentSqlExpr;
            sqlExpr = propertyExpr.getOwner();
        }
        if (sqlExpr instanceof SQLVariantRefExpr) {
            needCacheResult = true;
            return;
        }
    }

    private void parseQueryAutoCommitSQL() {
        if (Objects.nonNull(query.getFrom())) {
            return;
        }
        List<SQLSelectItem> selectItems = query.getSelectList();
        if (selectItems.size() != 1) {
            return;
        }
        SQLSelectItem selectItem = selectItems.iterator().next();
        SQLObject item = selectItem.getExpr();
        if (item instanceof SQLVariantRefExpr) {
            SQLVariantRefExpr expr = (SQLVariantRefExpr) item;
            if (!("autocommit".equalsIgnoreCase(expr.getName()) || "@@autocommit"
                .equalsIgnoreCase(expr.getName()))) {
                return;
            }
            queryGlobalAutoCommit = expr.isGlobal();
            queryAutoCommitHeader = Objects.nonNull(selectItem.getAlias()) ?
                selectItem.getAlias() :
                ShardingUtil.debugSql(expr);
            queryType = QUERY_TYPE.QUERY_AUTOCOMMIT;
        } else if (item instanceof SQLPropertyExpr) {
            SQLPropertyExpr expr = (SQLPropertyExpr) item;
            if (!"autocommit".equalsIgnoreCase(expr.getName())) {
                return;
            }
            queryGlobalAutoCommit = false;
            queryAutoCommitHeader = Objects.nonNull(selectItem.getAlias()) ?
                selectItem.getAlias() :
                ShardingUtil.debugSql(expr);
            queryType = QUERY_TYPE.QUERY_AUTOCOMMIT;
        }
    }

    private static class SeqsWhereConditionHandler extends WhereExprHandler {
        private Map<String, String> params = new HashMap<>();
        private final char literalSymbol;

        public SeqsWhereConditionHandler(char literalSymbol) {
            this.literalSymbol = literalSymbol;
        }

        @Override protected void handleIN(SQLInListExpr inExpr) {
        }

        @Override protected void hanldeEquality(SQLBinaryOpExpr e) {
            if (e == null) {
                return;
            }
            if (e.getOperator() != SQLBinaryOperator.Equality) {
                return;
            }
            SQLExpr left = e.getLeft();
            if (!(left instanceof SQLIdentifierExpr)) {
                return;
            }
            SQLExpr right = e.getRight();
            String value = null;
            try {
                value = ShardingUtil.getVal(right);
            } catch (Exception exception) {
                logger.error("Exception in ShardingSelectSQL.hanldeEquality()", exception);
                return;
            }
            String lowerName = ((SQLIdentifierExpr) left).getLowerName();
            // group 在PostgreSQL中是关键字,所以传入的时候必须带双引号,而识别功能时又需要去掉双引号
            lowerName = ShardingUtil.removeLiteralSymbol(lowerName, literalSymbol);
            params.put(lowerName, value);
        }
    }

    private void parseSeqsSQL(SQLSelectQueryBlock query) {
        if (!GeneratorUtil.DAL_DUAL.equalsIgnoreCase(tableName)) {
            return;
        }

        List<SQLSelectItem> items = query.getSelectList();
        // 单个或多个不连续 seq ID : next_value
        // 多个连续 seq ID : next_begin, next_end
        if (items.size() != 1 && items.size() != 2) {
            return;
        }
        if (!(items.get(0).getExpr() instanceof SQLIdentifierExpr)) {
            return;
        }
        if (items.size() == 2 && !(items.get(1).getExpr() instanceof SQLIdentifierExpr)) {
            return;
        }
        if (items.size() == 2) {
            SQLIdentifierExpr begin = (SQLIdentifierExpr) items.get(0).getExpr();
            SQLIdentifierExpr end = (SQLIdentifierExpr) items.get(1).getExpr();
            if (!Objects.isNull(begin) && !Objects.isNull(end)) {
                next_begin = begin.getLowerName();
                next_end = end.getLowerName();
            }
        } else {
            SQLIdentifierExpr e = (SQLIdentifierExpr) items.get(0).getExpr();
            if (e.getLowerName() != null) {
                selected_value = e.getLowerName();
            }
        }
        SeqsWhereConditionHandler handler = new SeqsWhereConditionHandler(literalSymbol);
        handler.parseWhere(query.getWhere());
        this.params.putAll(handler.params);
    }

    @Override protected void handleShardingSyntax() {
        //the seq sql will never go to handle state,the batch should not prevent the analysis of the seq sql
        parseSeqsSQL(query);
        //@formatter:off
        /*
         * Why SELECT needs batchAnalyze?
         * This is for a transaction sequence:
         *      insert into test (xxx) values (xxx), (xxx);
         *      select xxx from test;
         *      commit;
         * In order to put this `SELECT` directly to BatchServerSession without allowing
         * sharding logic to change the table name to sharding table name, `needSharding`
         * flag needs to be set to false to prevent from future sql hacking.
         * */
        //@formatter:on
        if (send2Batch.isNeedBatchAnalyze()) {
            needSharding = false;
            return;
        }
        if (!needSharding) {
            return;
        }
        parseSelectColumns(query.getSelectList());
        where = query.getWhere();
        whereHandler.parseWhere(query.getWhere());
        parseShardingValues();
    }

    private void handleLimit(SQLLimit l) {
        if (l == null) {
            return;
        }
        SQLExpr e = l.getRowCount();
        if (e == null) {
            return;
        }
        if (e instanceof SQLIntegerExpr) {
            limit = ((SQLIntegerExpr) e).getNumber().intValue();
            sqlFeature.setLimit(limit);
        }
        e = l.getOffset();
        if (e == null) {
            return;
        }
        if (e instanceof SQLIntegerExpr) {
            offset = ((SQLIntegerExpr) e).getNumber().intValue();
            sqlFeature.setOffset(offset);
        }
    }

    private void handleOrderBy(SQLOrderBy orderBy) {
        if (orderBy == null) {
            return;
        }
        sqlFeature.setOrderBy(true);
        List<SQLSelectOrderByItem> items = orderBy.getItems();
        items.forEach(item -> {
            if (item.getExpr() instanceof SQLPropertyExpr) {
                needRewriteOwnerInSelectedColuomns.add((SQLPropertyExpr) item.getExpr());
            }
        });
    }

    private void handleGroupBy(SQLSelectGroupByClause groupBy) {
        if (groupBy == null) {
            return;
        }
        sqlFeature.setGroupBy(true);
        for (SQLExpr groupByEx : groupBy.getItems()) {
            if (groupByEx instanceof SQLPropertyExpr) {
                groupByItems.add(ShardingUtil
                    .removeLiteralSymbol(((SQLPropertyExpr) groupByEx).getName(), literalSymbol));
                needRewriteOwnerInSelectedColuomns.add((SQLPropertyExpr) groupByEx);
            } else if (groupByEx instanceof SQLIdentifierExpr) {
                groupByItems.add(ShardingUtil
                    .removeLiteralSymbol(((SQLIdentifierExpr) groupByEx).getName(), literalSymbol));
            }
        }
    }

    private void parseSelectItem(SQLSelectItem x) {
        String name = null;
        AggregateFunc aggrMethod = AggregateFunc.NONE;
        if (x.getExpr() instanceof SQLPropertyExpr) {
            name = ((SQLPropertyExpr) x.getExpr()).getName();
            needRewriteOwnerInSelectedColuomns.add((SQLPropertyExpr) x.getExpr());
        } else if (x.getExpr() instanceof SQLIdentifierExpr) {
            name = ((SQLIdentifierExpr) x.getExpr()).getName();
        } else if (x.getExpr() instanceof SQLAggregateExpr) {
            SQLAggregateExpr aggr = (SQLAggregateExpr) x.getExpr();
            aggrMethod = AggregateFunc.valueOfFunc(aggr.getMethodName());
            if (SQLAggregateOption.DISTINCT == aggr.getOption()) {
                aggrMethod = AggregateFunc.DISTINCT;
            }
            aggr.getArguments().forEach(e -> {
                if (e instanceof SQLPropertyExpr) {
                    needRewriteOwnerInSelectedColuomns.add((SQLPropertyExpr) e);
                }
            });
        } else if (x.getExpr() instanceof SQLMethodInvokeExpr) {
            SQLMethodInvokeExpr methodE = (SQLMethodInvokeExpr) x.getExpr();
            aggrMethod = AggregateFunc.valueOfFunc(methodE.getMethodName().toUpperCase());
            methodE.getParameters().forEach(e -> {
                if (e instanceof SQLPropertyExpr) {
                    needRewriteOwnerInSelectedColuomns.add((SQLPropertyExpr) e);
                }
            });
        }
        aggrMethods.add(aggrMethod);
        if (x.getAlias() != null) {
            columnAlias.put(x.getAlias(), name);
        }
    }

    private void parseSelectColumns(List<SQLSelectItem> items) {
        items.forEach(x -> parseSelectItem(x));
    }

    private void handleAllSharding() {
        List<ShardingTable> allTables = shardingRouter.getAllShardingTable(tableNameLowerCase);
        if (allTables.isEmpty()) {
            return;
        }
        List<String> shardingIds = new ArrayList<>(allTables.size());
        allTables.forEach(i -> shardingIds.add(""));
        results = newShardingResult(allTables, shardingIds, 1);
        sqlFeature.setShardingSelectAll(true);
    }

    /**
     * 映射表根据映射key查询不到映射value的时候, 选择最后一片sharding分片发送sharding sql, 以获取空结果集给客户端
     */
    private void handleLastSharding() {
        results =
            new SelectShardingResult(getLastShardingTable(), Collections.singletonList(""), 1);
    }

    private void rewriteSelectedColumnsOwner(String shardedTable) {
        if (Objects.isNull(tableNameAlias)) {
            needRewriteOwnerInSelectedColuomns.forEach(x -> {
                ShardingUtil.checkLiteralSymbolAndReplaceTable(x, tableNameLowerCase,
                    (p) -> p.setOwner(new SQLIdentifierExpr(shardedTable)), literalSymbol);
            });
        }
    }

    /**
     * 处理传入的sharding in条件
     *
     * @param sqlInListExpr
     */
    private void parseShardingConditionInSQLExpr(String name, SQLInListExpr sqlInListExpr) {
        Objects.requireNonNull(sqlInListExpr, "sql in expr ast node must be not null");
        if (inContidon.shardingInAstNodeOp.isPresent()) {
            return;
        }
        // 当In的值个数小于阈值时不重写
        if (sqlInListExpr.getTargetList().size() < Constants.REWIRTE_IN_CONDITION_THRESHOLD) {
            return;
        }
        Map<String, SQLExpr> tempIdWithSQLExpr = new LinkedHashMap<>();
        sqlInListExpr.getTargetList().forEach(x -> {
            String idValue = ShardingUtil.getVal(x);
            tempIdWithSQLExpr.put(idValue, x);
        });
        inContidon.name = name;
        inContidon.idWithSQLExpr = tempIdWithSQLExpr;
        inContidon.shardingInAstNodeOp = Optional.of(sqlInListExpr);
    }

    private void parseExprParentIfSQLInListExpr(String name, SQLExpr expr) {
        if (Objects.isNull(expr.getParent())) {
            return;
        }
        if (!(expr.getParent() instanceof SQLExpr)) {
            logger.error(ShardingUtil.debugSql(expr.getParent()) + " is not instanceof SQLExpr");
            return;
        }
        SQLExpr condition = (SQLExpr) expr.getParent();
        if (condition instanceof SQLInListExpr) {
            parseShardingConditionInSQLExpr(name, (SQLInListExpr) condition);
        }
    }

    private void handleInShardingCondition(String name, SQLExpr expr, Set<String> values,
        Function<String, ShardingTable> f, SelectShardingResult selectShardingResult) {
        if (values.isEmpty()) {
            return;
        }
        parseExprParentIfSQLInListExpr(name, expr);
        if (inContidon.idWithSQLExpr.isEmpty()) {
            return;
        }
        // 以shardingTable维度group by id, id 可能是composed_key, sharding key。
        // 为了精确生成where in (?)的id值
        Map<String, List<String>> groupByedIdValues = new HashMap<>();
        values.forEach(value -> {
            ShardingTable t = f.apply(value);
            ensureShardingTableNotNull(t,
                () -> "ShardingTable of shardingColumn = " + name + " and id = " + value);
            groupByedIdValues.computeIfAbsent(t.table, table -> new LinkedList<>()).add(value);
        });
        selectShardingResult.setGroupByedIdValues(groupByedIdValues);
    }

    private void handleSelectVirtualShardingIndex(final String shardingKey) {
        ShardingTable singleTable = lookupShardingTableByShardingIndex(whereHandler.shardingIndex);
        SelectShardingResult selectShardingResult =
            new SelectShardingResult(Collections.singletonList(singleTable),
                Collections.singletonList(""), 1);
        results = selectShardingResult;
        //处理sharding select in的问题
        validShardingCondtion.composeColumnNameWithSQLExpr.forEach((col, expr) -> {
            Set<String> values = validShardingValues.composeColumnNameWithValues
                .getOrDefault(col, Collections.emptySet());
            handleInShardingCondition(col, expr, values, (v) -> shardingRouter
                    .getShardingTableByComposedKey(tableNameLowerCase, shardingKey, v),
                selectShardingResult);
        });
        validShardingCondtion.shardingKeyNameWithSQLExpr.forEach((col, expr) -> {
            if (Objects.equals(shardingKey, col)) {
                Set<String> values = validShardingValues.shardingKeyNameWithValues
                    .getOrDefault(col, Collections.emptySet());
                handleInShardingCondition(col, expr, values,
                    (v) -> shardingRouter.getShardingTable(tableNameLowerCase, shardingKey, v),
                    selectShardingResult);
            }
        });
    }

    @Override protected IterableShardingResult newShardingResult(List<ShardingTable> shardingTables,
        List<String> shardingIds, int shardingRuleCount) {
        return new SelectShardingResult(shardingTables, shardingIds, shardingRuleCount);
    }

    @Override protected void generateSQLs() {
        if (isLocked) {
            generateSQLsForUpdateDeleteSelectForUpdate();
        } else {
            generateSQLs4Select();
        }
    }

    private void handleSelectShardingByComposeKey(final String shardingKey) {
        Map.Entry<String, Set<String>> entry =
            validShardingValues.composeColumnNameWithValues.entrySet().iterator().next();
        String name4ComposeKey = entry.getKey();
        Set<String> values4ComposeKey = entry.getValue();
        Map<String, String> tableWithDb = new TreeMap<>();
        Map<String, String> tableWithComposeKeyValue = new HashMap<>();

        values4ComposeKey.forEach(value -> {
            final ShardingTable t = shardingRouter
                .getShardingTableByComposedKey(tableNameLowerCase, shardingKey, value);
            ensureShardingTableNotNull(t,
                () -> "ShardingTable == null of composed key = " + name4ComposeKey + " and value = "
                    + value);
            tableWithDb.put(t.table, t.database);
            tableWithComposeKeyValue.put(t.table, value);
        });
        List<ShardingTable> tables = new ArrayList<>();
        List<String> shardingIds = new ArrayList<>();
        // 经过Map以table维度去重后,对shard分片进行归并
        tableWithDb.forEach((table, db) -> {
            tables.add(new ShardingTable(table, db));
            String value = tableWithComposeKeyValue.get(table);
            shardingIds.add(shardingRouter.computeShardingId(tableNameLowerCase, value));
        });
        SelectShardingResult selectShardingResult =
            new SelectShardingResult(tables, shardingIds, 1);
        results = selectShardingResult;
        handleInShardingCondition(name4ComposeKey,
            validShardingCondtion.composeColumnNameWithSQLExpr.get(name4ComposeKey),
            values4ComposeKey,
            (v) -> shardingRouter.getShardingTableByComposedKey(tableNameLowerCase, shardingKey, v),
            selectShardingResult);
    }

    private void handleSelectShardingByShardingKey(final String shardingKey) {
        Set<String> values4ShardingKey =
            validShardingValues.shardingKeyNameWithValues.get(shardingKey);
        Map<String, String> tableWithDb = new TreeMap<>();
        Map<String, String> tableWithShardingKeyValue = new HashMap<>();
        values4ShardingKey.forEach(value -> {
            final ShardingTable t =
                shardingRouter.getShardingTable(tableNameLowerCase, shardingKey, value);
            ensureShardingTableNotNull(t,
                () -> "ShardingTable == null of sharding key = " + shardingKey + " and value = "
                    + value);
            tableWithDb.put(t.table, t.database);
            tableWithShardingKeyValue.put(t.table, value);
        });
        List<ShardingTable> tables = new ArrayList<>();
        List<String> shardingIds = new ArrayList<>();
        tableWithDb.forEach((table, db) -> {
            tables.add(new ShardingTable(table, db));
            String value = tableWithShardingKeyValue.get(table);
            shardingIds.add(shardingRouter.computeShardingId(value));
        });
        SelectShardingResult selectShardingResult =
            new SelectShardingResult(tables, shardingIds, 1);
        results = selectShardingResult;
        SQLExpr expr = validShardingCondtion.shardingKeyNameWithSQLExpr.get(shardingKey);
        if (Objects.isNull(expr)) {
            return;
        }
        handleInShardingCondition(shardingKey, expr, values4ShardingKey,
            (v) -> shardingRouter.getShardingTable(tableNameLowerCase, shardingKey, v),
            selectShardingResult);
    }

    private void handleSelectShardingByMappingKey() {
        Map.Entry<String, ShardingConfig.MappingKey> entry =
            mappingKeys.entrySet().iterator().next();
        ShardingConfig.MappingKey mKey = entry.getValue();
        Optional<List<String>> nameOp = findFirstMatchedMappingName(mKey.getMappingColumns(),
            validShardingValues.mappingKeyNameWithValues.keySet());
        Objects.requireNonNull(nameOp.isPresent(),
            () -> "strange! can not find mapping key for sharding from "
                + validShardingValues.mappingKeyNameWithValues.keySet());
        List<String> mappingColumns = nameOp.get();
        String name4MappingKey = String.join(",", mappingColumns);
        List<Set<String>> sortedMappingKeyValues = new ArrayList<>();
        for (String column : mappingColumns) {
            Set<String> mappingKeyValue = validShardingValues.mappingKeyNameWithValues.get(column);
            sortedMappingKeyValues.add(mappingKeyValue);
        }
        Set<String> values4MappingKey = new HashSet<>();
        for (int i = 0; i < sortedMappingKeyValues.size(); i++) {
            Set<String> oneMappingColumnValues = sortedMappingKeyValues.get(i);
            if (values4MappingKey.isEmpty()) {
                values4MappingKey.addAll(oneMappingColumnValues);
            } else {
                Set<String> composedValues4MappingKey = new HashSet<>();
                values4MappingKey.forEach(originValue -> {
                    oneMappingColumnValues.forEach(appendValue -> {
                        composedValues4MappingKey.add(originValue + "," + appendValue);
                    });
                });
                values4MappingKey = composedValues4MappingKey;
            }
        }
        //        Set<String> values4MappingKey = validShardingValues.mappingKeyNameWithValues.get(name4MappingKey.replaceAll(",", ""));
        // 此处隐含假定上述循环一定能找到一个mapping_rule用于sharding
        // 对于mapping_sharding，不对values做校验
        List<ShardingTable> tables = new ArrayList<>();
        List<SelectMappingEntity> selectMappingEntities = new ArrayList<>();
        final String mappingTablePrefix =
            mKey.getMappingRuleBySepName(name4MappingKey).getTablePrefix();
        for (String value : values4MappingKey) {
            ShardingTable t = shardingRouter
                .getMappingTableByMappingKey(tableNameLowerCase, mKey, name4MappingKey, value);
            ensureShardingTableNotNull(t, () -> String.format(
                "can not get ShardingTable when generate mapping sql, table=%s, columnName=%s, value=%s",
                tableNameAlias, name4MappingKey.replaceAll(",", ""), value));
            tables.add(t);
            SelectMappingEntity entity =
                new SelectMappingEntity(mappingTablePrefix, t, mKey.getColumn(),
                    name4MappingKey.replaceAll(",", ""), value, 0);
            selectMappingEntities.add(entity);
        }
        // mapping sharded sql 跳过shardingId校验,所以赋值其shardingId为空
        List<String> shardingIds = Collections.nCopies(tables.size(), "");
        mappingResults =
            new UpdateDeleteSelectMappingShardingResult(tables, selectMappingEntities, shardingIds,
                1);
        //清空,防止再次生成mapping sql
        validShardingValues.mappingKeyNameWithValues.clear();
    }

    private void generateSQLs4Select() {
        if (!needSharding) {
            return;
        }
        List<String> shardingKeys = composedKey.getShardingColumns();
        if (whereHandler.shardingIndex != -1) {
            String shardingKey = composedKey.getShardingColumns().get(0);
            handleSelectVirtualShardingIndex(shardingKey);
            return;
        }
        // 存在逻辑合法的composekey,则使用composekey sharding
        if (!validShardingValues.composeColumnNameWithValues.isEmpty()) {
            String shardingKey = composedKey.getShardingColumns().get(0);
            handleSelectShardingByComposeKey(shardingKey);
            return;
        }

        if (!validShardingValues.shardingKeyNameWithValues.isEmpty() && shardingKeys
            .containsAll(validShardingValues.shardingKeyNameWithValues.keySet())) {
            Optional<String> nameOp = findFirstMatchedName(shardingKeys,
                validShardingValues.shardingKeyNameWithValues.keySet());
            if (!nameOp.isPresent()) {
                throw new RuntimeException("strange! can not find sharding key for sharding from "
                    + validShardingValues.shardingKeyNameWithValues.keySet());
            }
            handleSelectShardingByShardingKey(nameOp.get());
            return;
        }

        // 隐含前置条件: where条件中既不含合法的composekey,也不含合法的shardingKey,有配置mapping_key
        // 一个mapping_key规则,且where 条件中包含至少一个合法的mappingkey表达式
        if (useMappingKeySharding()) {
            handleSelectShardingByMappingKey();
            return;
        }
        //mapping sharding select 场景,但是在映射表中并未查询到对应的值。
        if (useLastTableSharding()) {
            handleLastSharding();
            return;
        }
        handleAllSharding();
    }

    /**
     * 针对SELECT，多追加一种不需要mapping sharding的情况:
     * eg:
     * select * from tb_shiping_order where platform_tracking_id IN (V1,..., Vn)
     * 当映射表的key列 platform_tracking_id where 条件为 IN条件, 且IN条件中的值个数大于
     * sharding表的个数的时候不再尝试查询映射表,直接转换为全表扫描
     *
     * @return 是否选择mapping sharding
     */
    private boolean useMappingKeySharding() {
        if (validShardingValues.mappingKeyNameWithValues.isEmpty()) {
            return false;
        }
        int shardingTableSize = shardingRouter.getAllShardingTable(tableNameLowerCase).size();
        int mappingValuesSize =
            validShardingValues.mappingKeyNameWithValues.values().stream().mapToInt(v -> v.size())
                .max().orElse(0);
        return mappingValuesSize < shardingTableSize;
    }

    private boolean useLastTableSharding() {
        return !validShardingCondtion.mappingKeyNameWithSQLExpr.isEmpty()
            && validShardingValues.mappingKeyNameWithValues.isEmpty()
            && validShardingValues.shardingKeyNameWithValues.isEmpty();
    }

    @Override public void generateOriginalShardedSQLs() {
        generateSQLs();
        mostSafeSQL = originMostSafeSQL;
        whiteFields = originWhiteFields;
    }

    @Override public boolean rewriteSQLHints(List<SQLHint> hints) {
        SQLTableSource table = query.getFrom();
        if (Objects.isNull(table)) {
            return false;
        }
        if (!(table instanceof SQLExprTableSource)) {
            return false;
        }
        List<SQLHint> originHints = table.getHints();
        originHints.addAll(hints);
        ((SQLExprTableSource) table).setHints(originHints);
        SQLStatement selectStatement = (SQLStatement) query.getParent().getParent();
        //只重新进行基本语法解析，由于shardingSQL是延迟生成的,所以无需重新解析sharding规则
        handleSQLConstruct(this, ShardingUtil.debugSql(selectStatement), comment,
            Collections.singletonList(selectStatement));
        return true;
    }

    public class SelectShardingResult extends IterableShardingResult {
        private String firstTable;
        private String firstShardedSQL;
        private Map<String, List<String>> groupByedIdValues = Collections.emptyMap();
        private final List<String> shardingIds;

        public void setGroupByedIdValues(Map<String, List<String>> groupByedIdValues) {
            this.groupByedIdValues = groupByedIdValues;
        }

        public SelectShardingResult(List<ShardingTable> shardingTables, List<String> shardingIds,
            int shardingRuleCount) {
            super(shardingTables);
            this.shardingIds = shardingIds;
            this.shardingRuleCount = shardingRuleCount;
            this.aggrMethods = ShardingSelectSQL.this.aggrMethods;
            this.groupByItems = ShardingSelectSQL.this.groupByItems;
        }

        private boolean tryRewriteInTargetList(String shardedTable) {
            //当where in条件中的值个数小数阈值时不重写每条sharded sql中in的值
            if (inContidon.idWithSQLExpr.size() < Constants.REWIRTE_IN_CONDITION_THRESHOLD) {
                return false;
            }
            inContidon.shardingInAstNodeOp.ifPresent(sqlInListExpr -> {
                //@formatter:off
                List<SQLExpr> shardedTargetList = groupByedIdValues.getOrDefault(shardedTable, Collections.emptyList())
                        .stream()
                        .map(inContidon.idWithSQLExpr::get)
                        .collect(Collectors.toList());
                //@formatter:on
                //如果shard后targetList中值为空,则任选一个值填入，使其不语法报错。
                //这种场景在sharding_index规则的场景下可能出现
                if (shardedTargetList.isEmpty()) {
                    shardedTargetList.add(inContidon.idWithSQLExpr.values().iterator().next());
                }
                sqlInListExpr.setTargetList(shardedTargetList);
            });
            return inContidon.shardingInAstNodeOp.isPresent();
        }

        private ShardingResult generateFirstSQL() {
            // 1. generate the first SQL
            firstTable = shardingTables.get(0).table;
            String firstDB = shardingTables.get(0).database;
            tryRewriteInTargetList(firstTable);
            OwnerWhereExprHandler handler =
                new OwnerWhereExprHandler(tableNameLowerCase, literalSymbol);
            handler.parseWhere(where);
            if (Objects.isNull(tableNameAlias)) {
                handler.rewriteWhereOwners(firstTable);
            }
            rewriteTable(firstTable);
            rewriteSelectedColumnsOwner(firstTable);
            firstShardedSQL = addPrefixComment(comment, ShardingUtil.debugSql(sqlStmt));
            return buildResult(firstShardedSQL, firstTable, firstDB);
        }

        private ShardingResult generateOtherSQL() {
            if (index >= shardingTables.size()) {
                throw new NoSuchElementException();
            }
            // 2. replace the first table name to other table to optimize the performance
            // the solution is same in handleAllSharding()
            ShardingTable t = shardingTables.get(index);
            String shardedSqlSketch =
                tryRewriteInTargetList(t.table) ? ShardingUtil.debugSql(sqlStmt) : firstShardedSQL;
            String othershardedSQL = shardedSqlSketch.replace(firstTable, t.table);
            return buildResult(othershardedSQL, t.table, t.database);
        }

        private ShardingResult buildResult(String shardedSQL, String shardedTable,
            String shardedDB) {
            ShardingResult result = new ShardingResult(shardedSQL, shardedTable, shardedDB);
            result.id = shardingIds.get(index);
            if (isLocked) {
                result.shardingRuleIndex = index;
                result.shardingRuleCount = shardingRuleCount;
            }
            result.limit = limit;
            result.offset = offset;
            result.queryType = queryType;
            return result;
        }

        @Override public ShardingResult next() {
            ShardingResult r = null;
            if (index == 0) {
                r = generateFirstSQL();
            } else {
                r = generateOtherSQL();
            }
            index++;
            currentResult = r;
            return r;
        }

    }
}
