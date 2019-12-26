package me.ele.jarch.athena.sharding.sql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement.ValuesClause;
import com.github.mpjct.jmpjct.util.ErrorCode;
import io.etrace.agent.Trace;
import me.ele.jarch.athena.constant.TraceNames;
import me.ele.jarch.athena.exception.QueryException;
import me.ele.jarch.athena.sharding.ShardingConfig;
import me.ele.jarch.athena.sharding.ShardingHashCheckLevel;
import me.ele.jarch.athena.sharding.ShardingRouter;
import me.ele.jarch.athena.sharding.ShardingTable;
import me.ele.jarch.athena.sql.QUERY_TYPE;
import me.ele.jarch.athena.util.GreySwitch;
import me.ele.jarch.athena.util.etrace.EtracePatternUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ShardingInsertSQL extends ShardingSQL {
    private static Logger logger = LoggerFactory.getLogger(ShardingInsertSQL.class);
    private SQLInsertStatement insertStmt;
    private ShardingColumnsEntity entity = new ShardingColumnsEntity();
    private ShardingColumnType shardingColumnType = ShardingColumnType.COMPOSED_KEY;

    public ShardingInsertSQL(SQLObject sqlStmt, ShardingRouter shardingRouter) {
        super(sqlStmt, shardingRouter);
    }

    @Override protected void handleBasicSyntax() {
        queryType = QUERY_TYPE.INSERT;
        insertStmt = (SQLInsertStatement) sqlStmt;
        parseTable(insertStmt.getTableSource());
    }

    @Override protected void handleShardingSyntax() {
        if (send2Batch.resetAndGetNeedBatchAnalyze()) {
            needSharding = false;
            return;
        }
        if (!needSharding) {
            return;
        }
        shardingColumnType = computeShardingColumnType();
        parseColumns(insertStmt.getColumns());
        checkAllRowsOnSingleShardingId();
        checkSameShardingHash();
        parseValues(insertStmt.getValuesList());
    }

    /**
     * 校验insert的所有values是否都在同一个sharding id上
     * 只在values list == 1 或者已经被batch分析过的情况下跳过校验
     */
    private void checkAllRowsOnSingleShardingId() {
        if (insertStmt.getValuesList().size() == 1 || send2Batch.isShuffledByBatchContext()) {
            // batch分析过的sql不检查所有id的hash值是否相同，默认其相同
            return;
        }

        if (Objects.equals(ShardingColumnType.SHARDING_KEY, shardingColumnType)) {
            validateSameShardingIdByShardingKey();
        } else if (Objects.equals(ShardingColumnType.COMPOSED_KEY, shardingColumnType)) {
            validateSameShardingIdByComposedKey();
        }
    }

    private ShardingColumnType computeShardingColumnType() {
        if (Objects.nonNull(composedKey) && mappingKeys.size() > 1) {
            return ShardingColumnType.SHARDING_KEY;
        }
        return ShardingColumnType.COMPOSED_KEY;
    }

    /**
     * 此时存在多sharding key mapping,没有composeKey, 只能通过多个shardingkey组合出的
     * sharding id来校验所有value是否属于通过一个sharding id
     */
    private void validateSameShardingIdByShardingKey() {
        ensureHasEnoughShardingKeys();
        HashMap<String, List<String>> hashWithShardingKeyValues = new HashMap<>();
        insertStmt.getValuesList().forEach(valuesClause -> {
            StringJoiner shardingId = new StringJoiner(ShardingConfig.SHARDING_ID_DELIMITER);
            List<String> shardingKeyValues = new ArrayList<>(2);
            composedKey.getShardingColumns().forEach(shardingKey -> {
                int shardingKeyIndex = entity.shardingKeyNameWithIndex.get(shardingKey);
                String shardingKeyValue =
                    ShardingUtil.getVal(valuesClause.getValues().get(shardingKeyIndex));
                shardingKeyValues.add(shardingKeyValue);
                shardingId.add(shardingRouter.computeShardingId(shardingKeyValue));
            });
            hashWithShardingKeyValues.put(shardingId.toString(), shardingKeyValues);
        });
        if (hashWithShardingKeyValues.size() != 1) {
            throw new QueryException.Builder(ErrorCode.ERR_SQL_SYNTAX_ERROR).setErrorMessage(
                "insert multi rows with different sharding id is not allowed, id->value:"
                    + hashWithShardingKeyValues).bulid();
        }
    }

    /**
     * 通过insert语句中带的composedKey列的值校验insert的所有value是否属于同一个sharding id
     */
    private void validateSameShardingIdByComposedKey() {
        HashMap<String, String> hashWithId = new HashMap<>();
        insertStmt.getValuesList().forEach(valuesClause -> {
            entity.composeColumnNameWithIndex.forEach((col, index) -> {
                String id = ShardingUtil.getVal(valuesClause.getValues().get(index));
                hashWithId.put(shardingRouter.computeShardingId(tableNameLowerCase, id), id);
            });
        });

        if (hashWithId.size() != 1) {
            throw new QueryException.Builder(ErrorCode.ERR_SQL_SYNTAX_ERROR).setErrorMessage(
                "insert multi rows with different sharding id is not allowed, id->value:"
                    + hashWithId).bulid();
        }
    }

    /**
     * 验证composedkeys和shardingkeys是否属于同一个shardid
     */
    private void checkSameShardingHash() {
        ShardingHashCheckLevel shardingHashCheckLevel =
            GreySwitch.getInstance().getShardingHashCheckLevel();
        if (shardingHashCheckLevel == ShardingHashCheckLevel.PASS) {
            return;
        }

        // 验证该sql是否需要校验
        if (!isNeedValidateSharingHash()) {
            return;
        }

        for (ValuesClause valuesClause : insertStmt.getValuesList()) {
            Map<String, String> shardingHashMap = new HashMap<>();
            //get composedkey hashid
            entity.composeColumnNameWithIndex.forEach((col, index) -> {
                String composedKeyString = "%s , %s";
                String composedKeyValue = ShardingUtil.getVal(valuesClause.getValues().get(index));
                String composedKeyShardingHash =
                    shardingRouter.computeShardingId(tableNameLowerCase, composedKeyValue);
                composedKeyString = String.format(composedKeyString, col, composedKeyValue);
                shardingHashMap.put(composedKeyShardingHash, composedKeyString);
            });


            //get shardingkey hashid
            StringJoiner shardingKeyHashId = new StringJoiner(ShardingConfig.SHARDING_ID_DELIMITER);
            StringBuilder shardingKeyContent = new StringBuilder();

            composedKey.getShardingColumns().forEach(shardingKey -> {
                String shardingKeyString = "%s , %s";
                int shardingKeyIndex = entity.shardingKeyNameWithIndex.get(shardingKey);
                String shardingKeyValue =
                    ShardingUtil.getVal(valuesClause.getValues().get(shardingKeyIndex));
                String shardingKeyShardingHash = shardingRouter.computeShardingId(shardingKeyValue);
                shardingKeyContent
                    .append(String.format(shardingKeyString, shardingKey, shardingKeyValue));
                shardingKeyHashId.add(shardingKeyShardingHash);
            });
            shardingHashMap.put(shardingKeyHashId.toString(), shardingKeyContent.toString());

            //如果值不为1，则代表出现不同
            if (shardingHashMap.size() != 1) {
                StringBuilder traceDataContent = new StringBuilder();
                shardingHashMap.forEach((shardingHash, shardingHashContent) -> {
                    String shardingKeyString = "(%s , %s)";
                    traceDataContent.append(
                        String.format(shardingKeyString, shardingHashContent, shardingHash));
                });

                if (shardingHashCheckLevel == ShardingHashCheckLevel.WARN) {
                    Map<String, String> tags = new HashMap<>(1);
                    tags.put("tableName", tableName);
                    String sqlId = EtracePatternUtil.addAndGet(originMostSafeSQL).hash;


                    String traceContent = String
                        .format("sqlId : %s, (column, inputValue, shardingHash) %s", sqlId,
                            traceDataContent.toString());
                    Trace.logEvent(TraceNames.COMPOSEDKEY_NOT_MATCH_SHARDINGKEY, tableName,
                        io.etrace.common.Constants.FAILURE, traceContent, tags);
                } else if (shardingHashCheckLevel == ShardingHashCheckLevel.REJECT) {
                    throw new QueryException.Builder(ErrorCode.ERR_SQL_COMPOSEDKEY_ERROR)
                        .setErrorMessage(String.format(
                            "insert data composedkey not match shardingkey, tableName : %s, (column, inputValue, shardingHash) %s",
                            tableName, traceDataContent)).bulid();
                }
                break;
            }
        }
    }

    /*
     * 检查insert的table是否需要验证composed_key与sharding_key的shardid
     * */
    private boolean isNeedValidateSharingHash() {
        try {
            String tableName = tableNameLowerCase;
            //查询当前表的composedkey
            ShardingConfig.ComposedKey composedKey = shardingRouter.getComposedKey(tableName);
            //查询当前表的shardingkeys
            List<String> shardingKeys = shardingRouter.getShardingKeys(tableName);
            //获取当前表的generator
            String seq_name = shardingRouter.getComposedKey(tableName).getSeq_name();

            //是否包含所有shardingkey字段
            for (String shardingKey : shardingKeys) {
                if (!entity.shardingKeyNameWithIndex.containsKey(shardingKey)) {
                    return false;
                }
            }

            //当前table的route rule大于1 则校验
            Set<String> routingRuleSet = new HashSet<>();
            routingRuleSet.addAll(shardingKeys);
            routingRuleSet.addAll(composedKey.getColumns());

            return routingRuleSet.size() > 1;
        } catch (Exception e) {
            //如果校验出现问题则略过该校验
            logger.warn("validate shardid is failure, table is %s, err :", e);
            return false;
        }

    }

    /**
     * 遍历insert语句中的所有列,并提取出sharding相关列(composeKey, shardingKey, mappingKey, mappingValue)的名字以及索引信息.
     * 用于后续sharding使用
     *
     * @param list insert 语法树的列名对象
     */
    private void parseColumns(List<SQLExpr> list) {
        for (int i = 0; i < list.size(); i++) {
            String column = ShardingUtil.removeLiteralSymbol(list.get(i).toString(), literalSymbol);
            // 转换column为小写
            column = column.toLowerCase();
            if (composedKey.getColumns().contains(column)) {
                entity.composeColumnNameWithIndex.put(column, i);
            }
            if (composedKey.getShardingColumns().contains(column)) {
                entity.shardingKeyNameWithIndex.put(column, i);
            }
            if (mappingKeys.isEmpty()) {
                // 如果没有配置mapping_key,在跳过后续代码，进入下次循环
                continue;
            }
            // 提取mapping表的value
            if (mappingKeys.containsKey(column)) {
                entity.mappingValueWithIndex.put(column, i);
            }
            // 提取mapping表的key
            for (ShardingConfig.MappingKey mk : mappingKeys.values()) {
                if (mk.containsColumnName(column)) {
                    //遍历所有包含column的规则
                    List<ShardingConfig.MappingRule> mrs =
                        mk.getMappingRulesByOneColumnName(column);
                    for (ShardingConfig.MappingRule mr : mrs) {
                        entity.mappingKeyWithIndex.computeIfAbsent(mr.getComposedName(),
                            key -> new LinkedHashMap<>(mr.getOriginColumns().size()))
                            .put(mr.getOriginColumns().indexOf(column), i);
                    }
                }
            }
        }
    }

    /**
     * 提取sharding相关列(composeKey, shardingKey, mappingKey, mappingValue)
     * 的值并存储用于后续sharding.
     * 由于此方法调用前已经经过了values list值是否属于同一sharding id的校验,
     * 所以这里对于composeKey和shardingKey只提取insert values list的第一行values,
     * note: 对于经过batch分析后的SQL,只能保证valuesList的每一行在同一个shardingTable，
     * 不能保证在同一个shardingId,所以应用于shardingTable计算的时候只能取一行的values
     * 进行计算,来规避shardingId不同导致的事务校验失败
     * 但是对于mappingKey和mappingKey提取每一行insert valueColuses中的values
     *
     * @param valuesClauses 待提取值的insert values语法节点
     */
    private void parseValues(final List<ValuesClause> valuesClauses) {
        if (Objects.isNull(valuesClauses) || valuesClauses.isEmpty()) {
            return;
        }
        entity.composeColumnNameWithValue
            .putAll(index2Value(valuesClauses.get(0), entity.composeColumnNameWithIndex));
        entity.shardingKeyWithValue
            .putAll(index2Value(valuesClauses.get(0), entity.shardingKeyNameWithIndex));
        entity.mappingKeyWithValues
            .addAll(indexMap2Values(valuesClauses, entity.mappingKeyWithIndex));
        entity.mappingValueWithValues
            .addAll(index2Values(valuesClauses, entity.mappingValueWithIndex));
    }

    /**
     * 基于传入的列名和索引对,从ValuesClause对应位置提取值将其转换为列名和值的形式
     *
     * @param vc
     * @param columnWithIndex
     * @return
     */
    private Map<String, String> index2Value(final ValuesClause vc,
        Map<String, Integer> columnWithIndex) {
        if (Objects.isNull(columnWithIndex) || columnWithIndex.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, String> columnWithValue = new HashMap<>(2);
        columnWithIndex.forEach((column, index) -> {
            String value;
            try {
                value = ShardingUtil.getVal(vc.getValues().get(index));
            } catch (Exception e) {
                // 如果属于不支持取值的字段,也不可能用作composed key
                value = vc.getValues().get(index).toString();
            }
            columnWithValue.put(column, value);
        });
        return columnWithValue;
    }

    /**
     * 基于传入的列名和索引对,从List<ValuesClause>对应位置提取值将其转换为列名和值的List的形式
     *
     * @param vcs
     * @param columnWithIndex
     * @return
     */
    private List<Map<String, String>> index2Values(final List<ValuesClause> vcs,
        final Map<String, Integer> columnWithIndex) {
        if (Objects.isNull(columnWithIndex) || columnWithIndex.isEmpty()) {
            return Collections.emptyList();
        }
        final List<Map<String, String>> columnValues = new ArrayList<>();
        vcs.forEach(vc -> {
            Map<String, String> columnWithValue = new HashMap<>(columnWithIndex.size());
            columnWithValue.putAll(index2Value(vc, columnWithIndex));
            columnValues.add(columnWithValue);
        });
        return columnValues;
    }

    /**
     * 基于传入的列名和索引对,从List<ValuesClause>对应位置提取值将其转换为列名和值的List的形式
     *
     * @param vcs
     * @param columnWithIndexs
     * @return
     */
    private List<Map<String, List<String>>> indexMap2Values(final List<ValuesClause> vcs,
        final Map<String, Map<Integer, Integer>> columnWithIndexs) {
        if (Objects.isNull(columnWithIndexs) || columnWithIndexs.isEmpty()) {
            return Collections.emptyList();
        }
        final List<Map<String, List<String>>> columnValues = new ArrayList<>();
        vcs.forEach(vc -> {
            Map<String, List<String>> columnWithValue =
                new LinkedHashMap<>(columnWithIndexs.size());
            columnWithValue.putAll(indexMap2Value(vc, columnWithIndexs));
            columnValues.add(columnWithValue);
        });
        return columnValues;
    }

    private Map<String, List<String>> indexMap2Value(final ValuesClause vc,
        Map<String, Map<Integer, Integer>> columnWithIndex) {
        if (Objects.isNull(columnWithIndex) || columnWithIndex.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, List<String>> columnWithValue = new LinkedHashMap<>();
        Iterator it = columnWithIndex.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry entity = (Map.Entry) it.next();
            String column = (String) entity.getKey();
            Map<Integer, Integer> indexs = (Map<Integer, Integer>) entity.getValue();
            if (indexs == null || indexs.isEmpty()) {
                continue;
            }
            List<Integer> keyList = new ArrayList<>(indexs.keySet());
            keyList.sort(Comparator.comparingInt(key -> key));
            keyList.forEach(key -> {
                String value;
                try {
                    value = ShardingUtil.getVal(vc.getValues().get(indexs.get(key)));
                } catch (Exception e) {
                    // 如果属于不支持取值的字段,也不可能用作composed key
                    value = vc.getValues().get(indexs.get(key)).toString();
                }
                columnWithValue.computeIfAbsent(column, x -> new ArrayList<>()).add(value);
            });
        }
        return columnWithValue;
    }

    private boolean isNeedMappingSharding() {
        if (!needSharding) {
            return false;
        }
        if (Objects.isNull(composedKey)) {
            return false;
        }
        if (mappingKeys.isEmpty()) {
            return false;
        }
        if (entity.mappingValueWithIndex.isEmpty()) {
            return false;
        }
        if (entity.mappingKeyWithIndex.isEmpty()) {
            return false;
        }
        return true;
    }

    @Override protected void generateSQLs() {
        if (isNeedMappingSharding()) {
            generateMappingSQLs();
        } else {
            generateSQLsForInsert();
        }
    }

    // 确保通过shardingKey Insert时SQL带了所有要求的shardingKey, 否则抛异常
    private void ensureHasEnoughShardingKeys() {
        if (entity.shardingKeyNameWithIndex.size() < composedKey.getShardingColumns().size()) {
            throw new QueryException.Builder(ErrorCode.ERR_KEY_LACK_SHARDING_KEYS).setErrorMessage(
                "this sql need to be sharded by shardingKey but (" + entity.shardingKeyNameWithIndex
                    .keySet() + ") miss some shardingKey then expect shardingKey (" + composedKey
                    .getShardingColumns() + ")").bulid();
        }
    }

    private void generateSQLsForInsertByComposeKey() {
        if (entity.composeColumnNameWithValue.isEmpty()) {
            throw new QueryException.Builder(ErrorCode.ERR_NO_COMPOSED_ID).setErrorMessage(
                "this sql need to be sharded but cannot extract composed key: " + composedKey
                    .getColumn()).bulid();
        }
        //多 composed key 的 insert 语句必须传入所有的composedkey
        if (!entity.composeColumnNameWithValue.keySet().containsAll(composedKey.getColumns())) {
            throw new QueryException.Builder(ErrorCode.ERR_KEY_IS_LACK_MUTLI_COMPOSED_KEYS)
                .setErrorMessage(
                    "this sql need to be sharded and should give all composed keys(" + composedKey
                        .getColumns() + ")").bulid();
        }
        List<ShardingTable> tables = new ArrayList<>();
        List<String> shardingIds = new ArrayList<>(2);
        composedKey.getShardingColumns().forEach((shardingKey) -> {
            Set<ShardingTable> allComposeKeyTables = new HashSet<>();
            Set<String> allComposeKeyShardingIds = new HashSet<>();
            // 无论单composeKey还是多composeKey都可统一处理
            entity.composeColumnNameWithValue.forEach((ckey, value) -> {
                ShardingTable shardingTable = shardingRouter
                    .getShardingTableByComposedKey(tableNameLowerCase, shardingKey, value);
                ensureShardingTableNotNull(shardingTable,
                    () -> "ShardingTable of composed key == null of composeKey = " + ckey
                        + " value = " + value);
                allComposeKeyTables.add(shardingTable);
                allComposeKeyShardingIds
                    .add(shardingRouter.computeShardingId(tableNameLowerCase, value));
            });
            //检查多composeKey 的情况下,是否所有的composeKey 的值计算出的sharding table是否相同
            //如果不相同,必须报错,不允许插入,否则后续的查找将无法正常找到
            if (allComposeKeyTables.size() != 1 || allComposeKeyShardingIds.size() != 1) {
                throw new QueryException.Builder(
                    ErrorCode.ERR_MULTI_COMPOSED_KEY_NOT_SAME_SHARDING_TABLE).setErrorMessage(
                    "multi composed key ( " + composedKey.getColumns()
                        + " ) value not in same sharding table").bulid();
            }
            tables.addAll(allComposeKeyTables);
            shardingIds.addAll(allComposeKeyShardingIds);
        });
        results =
            new InsertShardingResult(tables, composedKey.getShardingColumns().size(), shardingIds);
        if (composedKey.getShardingColumns().size() >= 2) {
            Map<String, Set<String>> keyWithValues = new HashMap<>(2);
            entity.composeColumnNameWithValue
                .forEach((k, v) -> keyWithValues.put(k, Collections.singleton(v)));
            results.originShardingKey = keyWithValues;
        }
    }

    private void generateSQLsForInsertByTwoShardingKeys() {
        ensureHasEnoughShardingKeys();
        List<ShardingTable> tables = new ArrayList<>();
        StringJoiner shardingId = new StringJoiner(ShardingConfig.SHARDING_ID_DELIMITER);
        composedKey.getShardingColumns().forEach((shardingKey) -> {
            String value = entity.shardingKeyWithValue.get(shardingKey);
            final ShardingTable shardingTable =
                shardingRouter.getShardingTable(tableNameLowerCase, shardingKey, value);
            ensureShardingTableNotNull(shardingTable,
                () -> "ShardingTable of shardingKey == null of " + entity.shardingKeyWithValue);
            shardingId.add(shardingRouter.computeShardingId(value));
            tables.add(shardingTable);
        });
        results = new InsertShardingResult(tables, composedKey.getShardingColumns().size(),
            Collections.nCopies(tables.size(), shardingId.toString()));
        if (composedKey.getShardingColumns().size() >= 2) {
            Map<String, Set<String>> keyWithValues = new HashMap<>(2);
            entity.shardingKeyWithValue
                .forEach((k, v) -> keyWithValues.put(k, Collections.singleton(v)));
            results.originShardingKey = keyWithValues;
        }
    }

    private void generateSQLsForInsert() {
        if (!needSharding) {
            return;
        }

        if (Objects.equals(shardingColumnType.COMPOSED_KEY, shardingColumnType)) {
            generateSQLsForInsertByComposeKey();
        } else if (Objects.equals(ShardingColumnType.SHARDING_KEY, shardingColumnType)) {
            generateSQLsForInsertByTwoShardingKeys();
        }
    }

    private void generateMappingSQLs() {
        List<ShardingTable> tables = new ArrayList<>();
        List<InsertMappingEntity> entities = new ArrayList<>();
        for (int i = 0; i < entity.mappingValueWithValues.size(); i++) {
            // 只使用其计数功能，无需其原子性
            final AtomicInteger shardingRuleIndex = new AtomicInteger(0);
            final Map<String, List<String>> mappingKeyWithValue =
                entity.mappingKeyWithValues.get(i);
            final Map<String, String> mappingValueWithValue = entity.mappingValueWithValues.get(i);
            mappingKeys.forEach((name4MappingValue, mKey) -> {
                final String value4MappingValue = mappingValueWithValue.get(name4MappingValue);
                // 如果mapping表的value必然在SQL中存在至少一个,否则不会生成mapping SQL
                mKey.getMappingRuleValues().forEach(mappingRule -> {
                    String mappingKeyColumnName = mappingRule.getComposedName();
                    List<String> value4MappingKey = mappingKeyWithValue.get(mappingKeyColumnName);

                    if (value4MappingKey == null || value4MappingKey.isEmpty()) {
                        // 如果mapping表的key在insert sql中不存在,则跳过生成mapping sql
                        return;
                    }
                    String valueString = String.join(",", value4MappingKey);
                    ShardingTable t = shardingRouter
                        .getMappingTableByMappingKey(tableNameLowerCase, mKey,
                            mappingRule.getSepColumnName(), valueString);
                    ensureShardingTableNotNull(t, () -> String.format(
                        "can not get ShardingTable when generate mapping sql, table=%s, columnName=%s, value=%s",
                        tableNameLowerCase, mappingKeyColumnName, value4MappingKey));
                    tables.add(t);
                    entities.add(new InsertMappingEntity(mappingRule.getTablePrefix(), t,
                        mappingKeyColumnName, valueString, name4MappingValue, value4MappingValue,
                        shardingRuleIndex.get()));
                });
                shardingRuleIndex.incrementAndGet();
            });
        }
        // mapping sharded sql 跳过shardingId校验,所以赋值其shardingId为空
        List<String> shardingIds = Collections.nCopies(tables.size(), "");
        mappingResults = new InsertMappingShardingResult(tables, entities, shardingIds,
            composedKey.getShardingColumns().size());
    }

    @Override public void generateOriginalShardedSQLs() {
        generateSQLsForInsert();
        mostSafeSQL = originMostSafeSQL;
        whiteFields = originWhiteFields;
    }

    private class InsertMappingEntity {
        /**
         * 所使用的mapping表前缀
         */
        public final String mappingTablePrefix;
        public final ShardingTable shardingTable;
        public final String name4MappingKey;
        public final String value4MappingKey;
        public final String name4MappingValue;
        public final String value4MappingValue;
        /**
         * 多维sharding时,用于标识关联到哪一维度上
         */
        public final int shardingRuleIndex;

        public InsertMappingEntity(String mappingTablePrefix, ShardingTable shardingTable,
            String name4MappingKey, String value4MappingKey, String name4MappingValue,
            String value4MappingValue, int shardingRuleIndex) {
            this.mappingTablePrefix = mappingTablePrefix;
            this.shardingTable = shardingTable;
            this.name4MappingKey = name4MappingKey;
            this.value4MappingKey = value4MappingKey;
            this.name4MappingValue = name4MappingValue;
            this.value4MappingValue = value4MappingValue;
            this.shardingRuleIndex = shardingRuleIndex;
        }
    }


    public class InsertShardingResult extends IterableShardingResult {
        private final List<String> shardingIds;

        public InsertShardingResult(List<ShardingTable> shardingTables, int shardingRuleCount,
            List<String> shardingIds) {
            super(shardingTables, shardingRuleCount);
            this.shardingIds = shardingIds;
        }

        @Override public ShardingResult next() {
            if (index >= shardingTables.size()) {
                throw new NoSuchElementException();
            }
            ShardingTable t = shardingTables.get(index);
            String table = t.table;
            String database = t.database;
            rewriteTable(table);
            String shardedSql = addPrefixComment(comment, ShardingUtil.debugSql(sqlStmt));
            ShardingResult r = new ShardingResult(shardedSql, table, database);
            r.id = shardingIds.get(index);
            r.shardingRuleCount = shardingRuleCount;
            r.shardingRuleIndex = index;
            r.queryType = queryType;
            currentResult = r;
            index++;
            return r;
        }
    }


    public class InsertMappingShardingResult extends IterableShardingResult {
        private static final String INSERT_MAPPING_SQL_TEMPLATE =
            "INSERT INTO %s (%s, %s) VALUES (%s, %s) ON DUPLICATE KEY UPDATE %s = %s";
        private static final String SQL_VARCHAR_TEMPLATE = "'%s'";
        private static final String WHITE_FIELDS_TEMPLATE = "%s->%s#%s->%s";

        private List<InsertMappingEntity> entities;
        private final List<String> shardingIds;

        public InsertMappingShardingResult(List<ShardingTable> shardingTables,
            List<InsertMappingEntity> entities, List<String> shardingIds, int shardingRuleCount) {
            super(shardingTables, shardingRuleCount);
            this.entities = entities;
            this.shardingIds = shardingIds;
        }

        @Override public ShardingResult next() {
            if (index >= shardingTables.size()) {
                throw new NoSuchElementException();
            }
            InsertMappingEntity entity = entities.get(index);
            String column4MappingKey = entity.name4MappingKey;
            String column4MappingValue = entity.name4MappingValue;
            String columnValue4MappingKey =
                String.format(SQL_VARCHAR_TEMPLATE, entity.value4MappingKey);
            String columnValue4MappingValue =
                String.format(SQL_VARCHAR_TEMPLATE, entity.value4MappingValue);
            String table = entity.shardingTable.table;
            String database = entity.shardingTable.database;
            String shardedSql = String
                .format(INSERT_MAPPING_SQL_TEMPLATE, table, column4MappingKey, column4MappingValue,
                    columnValue4MappingKey, columnValue4MappingValue, column4MappingValue,
                    columnValue4MappingValue);
            ShardingResult r = new ShardingResult(shardedSql, table, database);
            String tablePrefix = entity.mappingTablePrefix;
            rewriteMostSafeSQL(tablePrefix, column4MappingKey, column4MappingValue);
            rewriteWhiteFields(column4MappingKey, columnValue4MappingKey, column4MappingValue,
                columnValue4MappingValue);
            r.id = shardingIds.get(index);
            r.shardingRuleCount = shardingRuleCount;
            r.shardingRuleIndex = entity.shardingRuleIndex;
            r.queryType = queryType;
            currentResult = r;
            index++;
            return r;
        }

        private void rewriteMostSafeSQL(String tablePrefix, String column4MappingKey,
            String column4MappingValue) {
            mostSafeSQL = String.format(INSERT_MAPPING_SQL_TEMPLATE, tablePrefix, column4MappingKey,
                column4MappingValue, "?", "?", column4MappingValue, "?");
        }

        private void rewriteWhiteFields(String column4MappingKey, String columnValue4MappingKey,
            String column4MappingValue, String columnValue4MappingValue) {
            whiteFields = String
                .format(WHITE_FIELDS_TEMPLATE, column4MappingKey, columnValue4MappingKey,
                    column4MappingValue, columnValue4MappingValue);
        }
    }


    /**
     * 用于INSERT类型的Sharding标记根据1个composeKey还是2个shardingKey sharding
     */
    enum ShardingColumnType {
        COMPOSED_KEY, // 通过composedKey列sharding
        SHARDING_KEY // 通过shardingkey列sharding
    }


    /**
     * 用于INSERT类型SQL分门别类的收集sharding列信息
     */
    class ShardingColumnsEntity {
        public final Map<String, Integer> composeColumnNameWithIndex = new HashMap<>(2);
        public final Map<String, String> composeColumnNameWithValue = new HashMap<>(2);
        public final Map<String, Integer> shardingKeyNameWithIndex = new HashMap<>(2);
        public final Map<String, String> shardingKeyWithValue = new HashMap<>(2);
        public final Map<String, Integer> mappingValueWithIndex = new HashMap<>(2);
        public final List<Map<String, String>> mappingValueWithValues = new ArrayList<>(2);
        public final Map<String, Map<Integer, Integer>> mappingKeyWithIndex =
            new LinkedHashMap<>(2);
        public final List<Map<String, List<String>>> mappingKeyWithValues = new ArrayList<>(2);
    }

}
