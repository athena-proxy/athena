package me.ele.jarch.athena.sharding.sql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLHint;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.dialect.postgresql.ast.stmt.PGSelectStatement;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.github.mpjct.jmpjct.util.ErrorCode;
import me.ele.jarch.athena.allinone.DBVendor;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.exception.QueryException;
import me.ele.jarch.athena.sharding.ShardingConfig.ComposedKey;
import me.ele.jarch.athena.sharding.ShardingConfig.MappingKey;
import me.ele.jarch.athena.sharding.ShardingRouter;
import me.ele.jarch.athena.sharding.ShardingTable;
import me.ele.jarch.athena.sharding.sql.ShardingUtil.SafeSQL;
import me.ele.jarch.athena.sharding.sql.dialect.postgresql.parser.DalPGSQLStatementParser;
import me.ele.jarch.athena.sql.QUERY_TYPE;
import me.ele.jarch.athena.util.GreySwitch;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static me.ele.jarch.athena.sharding.ShardingConfig.SHARDING_ID_DELIMITER;

public abstract class ShardingSQL {
    private static final Logger logger = LoggerFactory.getLogger(ShardingSQL.class);
    public static final Predicate<String> BLACK_FIELDS_FILTER = field -> false;

    protected final ShardingRouter shardingRouter;
    /* whether cache result for this sql*/
    public boolean needCacheResult = false;
    // whether this sql should be sharded
    public boolean needSharding = false;
    // decide whether the sql need batchAnalyze
    public Send2BatchCond send2Batch;
    // table name in sql, e.g. Eleme_order

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getComment() {
        return comment;
    }

    public String tableName;
    // lower case of the table name, for invoke ShardingRouter API
    // e.g. eleme_order
    public String tableNameLowerCase;
    // table alias in sql, e.g. Eleme_order AS t1
    public String tableNameAlias;
    public String specifyDB = "";
    protected Map<String, String> columnAlias = new HashMap<>();
    protected final SQLObject sqlStmt;
    public IterableShardingResult results = IterableShardingResult.EMPTY_ITERABLE_SHARDING_RESULT;
    protected IterableShardingResult mappingResults =
        IterableShardingResult.EMPTY_ITERABLE_SHARDING_RESULT;
    protected String comment = "";
    // origin sql which to be sharded
    public String originSQL;
    /**
     * 完全脱敏后的sql，主要用于etrace，也用于athena_pattern_map.log,值表示当前执行的Sharding SQL的mostSafeSQL
     * mapping sharding sql 执行途中,值可能会被改为mapping sql的mostSafeSQL
     */
    protected String mostSafeSQL = "";
    // Client原始SQL的pattern,为了mapping sharding SQL执行完成后,将mostSafeSQL置回业务SQL的pattern
    protected String originMostSafeSQL = "";
    // 当前执行的Sharding SQL的whiteFields, mapping sharding SQL执行时为mapping SQL的whiteFields
    protected String whiteFields = "";
    // 原始的Sharding SQL的whiteFields, 总是业务SQL的white Fields
    protected String originWhiteFields = "";
    public int autocommitValue = -1;
    // 业务需求: 如果SQL里包含where 'bind_master' = 'bind_master',那么这个字段设置为true
    public boolean isContainsBindMasterExpr = false;
    // 是否为insert语句添加了ezone_shard_info列
    public boolean isEZoneShardInfoRewrited = false;

    // only for select and update and delete
    protected SQLExpr where;
    protected ConditionWhereExprHandler whereHandler = new ConditionWhereExprHandler(this);
    protected ComposedKey composedKey;
    /**
     * SQL 表所关联的所有mapping_keys, 顺序按照配置的书写顺序
     * 结构:
     * mapping表的value : mapping规则
     */
    protected final LinkedHashMap<String, MappingKey> mappingKeys = new LinkedHashMap<>();
    // 表名为dual_dual时,所有where条件后的key=value键值对,给globalid逻辑使用
    // 该Map为不为空时,表示时globalid的sql
    public final Map<String, String> params = new HashMap<>();
    //值为next_value或sharding_count或unsafe_on等
    public String selected_value = "";
    //下面的两个值是在 golbalid range 连续 globalid时使用
    //此时selected_value 一个值不够
    public String next_begin = "";
    public String next_end = "";
    // 当sql为kill [query|connection] threadId时有效
    // 当sql是kill,clientIdForKill仍然为-1时,表示sql命令有问题,如发了kill query 1+1
    protected long clientIdForKill = -1;
    //是否查询全局的Autocommit属性
    protected boolean queryGlobalAutoCommit = false;
    //伪造Autocommit结果的列名
    protected String queryAutoCommitHeader = "@@autocommit";
    protected Predicate<String> whiteFieldsFilter = BLACK_FIELDS_FILTER;
    // 从where条件中收集到的可能用于sharding的条件
    protected final ShardingCondition shardingCondition = new ShardingCondition();
    // 经过 and or逻辑校验后,可用于sharding的条件
    protected final ShardingCondition validShardingCondtion = new ShardingCondition();
    // sharding条件名字及其值
    protected final ShardingValues validShardingValues = new ShardingValues();
    public QUERY_TYPE queryType = QUERY_TYPE.OTHER;

    /**
     * 字面符号,用于禁止对于SQL中的标识符进行关键字检查,如果数据库检查出SQL语句中包含预定义的
     * 关键字,会将SQL中的标识符安装关键字的含义进行处理,可能会出现未预期的结果(通常是报错),此
     * 时可以使用字面符号包裹标识符来禁止关键字检查。
     * <a href="https://dev.mysql.com/doc/refman/5.7/en/identifiers.html">对应与MySQL语法中的反引号</a>
     * <a href="https://www.postgresql.org/docs/9.3/static/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS">对应于PostgreSQL语法中的双引号</a>
     * 默认遵循MySQL的规则
     */
    protected final char literalSymbol;

    public final SQLFeature sqlFeature;

    public boolean isQueryGlobalAutoCommit() {
        return queryGlobalAutoCommit;
    }

    public String getQueryAutoCommitHeader() {
        return queryAutoCommitHeader;
    }

    public long getClientIdForKill() {
        return clientIdForKill;
    }

    public ShardingSQL(SQLObject sqlStmt, ShardingRouter shardingRouter) {
        this.sqlStmt = sqlStmt;
        this.shardingRouter = shardingRouter;
        boolean isPostgreSQL = ShardingUtil.isPGObject(sqlStmt);
        literalSymbol = isPostgreSQL ? '"' : '`';
        sqlFeature = new SQLFeature(isPostgreSQL ? DBVendor.PG : DBVendor.MYSQL);
    }

    protected void checkBindMasterExpr(SQLExpr whereExpr) {
        BindMasterWhereExprHandler handler = new BindMasterWhereExprHandler(this);
        handler.parseWhere(whereExpr);
    }

    protected void checkSingleConditionSQL(SQLExpr whereExpr) {
        if (Objects.isNull(whereExpr)) {
            return;
        }
        sqlFeature.setWhere(true);
        if (whereExpr instanceof SQLInListExpr) {
            SQLInListExpr inListExpr = (SQLInListExpr) whereExpr;
            if (inListExpr.isNot()) {
                return;
            }
            sqlFeature.setSingleInValues(inListExpr.getTargetList().size());
        } else if (whereExpr instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr w = (SQLBinaryOpExpr) whereExpr;
            if (w.getOperator() != SQLBinaryOperator.Equality) {
                return;
            }
            if (!(w.getLeft() instanceof SQLIdentifierExpr)) {
                return;
            }
            sqlFeature.setOneKey(true);
        } else {
            // NOOP
        }
    }

    /**
     * 检查pName是否是columns的一个元素(忽略大小写)
     *
     * @param pName   待检查的名字
     * @param columns 预期的sharding相关列集合
     * @return
     */
    private static boolean checkShardingColumn(final String pName,
        final Collection<String> columns) {
        if (Objects.isNull(pName)) {
            return false;
        }
        return columns.stream().anyMatch(pName::equalsIgnoreCase);
    }

    /**
     * 检查pName是否是多个字段做mapping key
     *
     * @param pName   待检查的名字
     * @param columns 预期的mapping相关列集合
     * @return
     */
    private static boolean checkComposedColumn4Mapping(final String pName,
        final Map<String, List<String>> columns) {
        if (Objects.isNull(pName)) {
            return false;
        }
        if (columns.keySet().stream().anyMatch(pName::equalsIgnoreCase)) {
            return true;
        }
        for (List<String> composedColumns : columns.values()) {
            if (composedColumns.size() < 2) {
                continue;
            }
            for (String column : composedColumns) {
                if (pName.equals(column)) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean innerExprNeedRewrite(String pName, SQLExpr p) {
        // e.g. select restaurant_id as r1 FROM eleme_order WHERE r1 = 100
        // columnAlias is a map [r1->restaurant_id]
        // p.getName() is r1 in [WHERE r1 = 100]
        // pAlias is restaurant_id in [restaurant_id as r1]
        String pAlias = columnAlias.get(pName);
        if (pAlias != null) {
            pName = pAlias;
        }
        pName = ShardingUtil.removeLiteralSymbol(pName, literalSymbol);
        // check composed Key column
        // composed key 在sharding相关列中优先级最高
        if (checkShardingColumn(pName, composedKey.getColumns())) {
            // 如果找到一个composedKey,则先存储其名字及其关联语法节点
            // 存储的名字统一为小写格式
            shardingCondition.composeColumnNameWithSQLExpr.put(pName.toLowerCase(), p);
            return true;
        }
        if (this instanceof ShardingSelectSQL || this instanceof ShardingUpdateSQL
            || this instanceof ShardingDeleteSQL) {
            List<String> columns = shardingRouter.getShardingKeys(tableNameLowerCase);
            if (checkShardingColumn(pName, columns)) {
                // 如果找到一个composedKey,则先存储其名字及其关联语法节点
                // 存储的名字统一为小写格式
                shardingCondition.shardingKeyNameWithSQLExpr.put(pName.toLowerCase(), p);
                return true;
            }
        }
        return false;
    }

    private boolean innerExprNeedCollect4Mapping(String pName, SQLExpr p) {
        String pAlias = columnAlias.get(pName);
        if (pAlias != null) {
            pName = pAlias;
        }
        pName = ShardingUtil.removeLiteralSymbol(pName, literalSymbol);
        // check mapping Key column
        // 如果不存在配置的mapping_key, 则提前返回
        if (mappingKeys.isEmpty()) {
            return false;
        }
        for (MappingKey mKey : mappingKeys.values()) {
            //检查是否符合mapping key
            if (checkComposedColumn4Mapping(pName, mKey.getMappingColumns())) {
                // 存储的名字统一为小写格式
                shardingCondition.mappingKeyNameWithSQLExpr.put(pName.toLowerCase(), p);
                return true;
            }
        }
        return false;
    }

    // check whether the expression need rewrite
    // e.g.
    // eleme_order.restaurant_id = 100 return true
    // restaurant_id = 100 return true
    // a.b =200 return false
    boolean exprNeedRewrite(SQLExpr e, boolean is4Mapping) {
        if (Objects.isNull(e)) {
            return false;
        }
        // 此函数有相似代码,因为SQLPropertyExpr和SQLIdentifierExpr虽然有getName()方法,但没有继承关系
        if (e instanceof SQLPropertyExpr) {
            // eleme_order.restaurant_id
            SQLPropertyExpr property = (SQLPropertyExpr) e;
            if (Objects.isNull(property.getName())) {
                return false;
            }
            if (is4Mapping) {
                return innerExprNeedCollect4Mapping(property.getName(), property);
            } else {
                return innerExprNeedRewrite(property.getName(), property);
            }
        } else if (e instanceof SQLIdentifierExpr) {
            // restaurant_id
            SQLIdentifierExpr identifier = (SQLIdentifierExpr) e;
            if (Objects.isNull(identifier.getName())) {
                return false;
            }
            if (is4Mapping) {
                return innerExprNeedCollect4Mapping(identifier.getName(), identifier);
            } else {
                return innerExprNeedRewrite(identifier.getName(), identifier);
            }
        }
        return false;
    }

    /**
     * 从对where 条件的语法分析和逻辑分析中得到的有效sharding条件中
     * 提取这些条件对应的值用于后续sharding计算
     */
    protected void parseShardingValues() {
        validShardingCondtion.composeColumnNameWithSQLExpr.forEach((col, expr) -> {
            Set<String> vSet = parseValue(expr);
            if (!vSet.isEmpty()) {
                validShardingValues.composeColumnNameWithValues.put(col, vSet);
            }
        });
        validShardingCondtion.shardingKeyNameWithSQLExpr.forEach((col, expr) -> {
            Set<String> vSet = parseValue(expr);
            if (!vSet.isEmpty()) {
                validShardingValues.shardingKeyNameWithValues.put(col, vSet);
            }
        });
        validShardingCondtion.mappingKeyNameWithSQLExpr.forEach((col, expr) -> {
            Set<String> vSet = parseValue(expr);
            if (!vSet.isEmpty()) {
                validShardingValues.mappingKeyNameWithValues.put(col, vSet);
            }
        });
    }

    /**
     * 解析=，in 条件表达式关联的值
     *
     * @param expr 条件表达式
     * @return 值set, 异常情况下返回空set.
     */
    private Set<String> parseValue(SQLExpr expr) {
        if (Objects.isNull(expr.getParent())) {
            return Collections.emptySet();
        }
        if (!(expr.getParent() instanceof SQLExpr)) {
            logger.error("{} is not instanceof SQLExpr", ShardingUtil.debugSql(expr.getParent()));
            return Collections.emptySet();
        }
        SQLExpr cond = (SQLExpr) expr.getParent();
        if (cond instanceof SQLBinaryOpExpr) {
            return Collections.singleton(ShardingUtil.getVal(cond));
        } else if (cond instanceof SQLInListExpr) {
            SQLInListExpr inListExpr = (SQLInListExpr) cond;
            // 此处使用LinkedHashSet是希望尽可能在改写SQL时保留原始SQL值的顺序信息
            Set<String> vSet = new LinkedHashSet<>();
            inListExpr.getTargetList().forEach(x -> vSet.add(ShardingUtil.getVal(x)));
            return vSet;
        } else {
            logger.error("{} is not SQLBinaryOpExpr or SQLInListExpr",
                ShardingUtil.debugSql(expr.getParent()));
            return Collections.emptySet();
        }
    }

    // parse the TABLE in SQL
    // e.g. UPDATE TABLE,SELECT * FROM TABLE,INSERT INTO TABLE
    protected void parseTable(SQLTableSource table) {
        // not support multi tables
        if (!(table instanceof SQLExprTableSource)) {
            needSharding = false;
            return;
        }
        // only support single table
        SQLExpr e = ((SQLExprTableSource) table).getExpr();
        if (e instanceof SQLIdentifierExpr) {
            tableName =
                ShardingUtil.removeLiteralSymbol(((SQLIdentifierExpr) e).getName(), literalSymbol);
            tableNameAlias = ShardingUtil.removeLiteralSymbol(table.getAlias(), literalSymbol);
            tableNameLowerCase = tableName.toLowerCase();
        } else if (e instanceof SQLPropertyExpr) {
            tableName =
                ShardingUtil.removeLiteralSymbol(((SQLPropertyExpr) e).getName(), literalSymbol);
            tableNameAlias = ShardingUtil.removeLiteralSymbol(table.getAlias(), literalSymbol);
            tableNameLowerCase = tableName.toLowerCase();
            if (StringUtils.isNotEmpty(((SQLPropertyExpr) e).getOwnernName())) {
                specifyDB = ShardingUtil
                    .removeLiteralSymbol(((SQLPropertyExpr) e).getOwnernName(), literalSymbol);
            }
        } else {
            throw new QueryException.Builder(ErrorCode.ERR_SQL_SYNTAX_ERROR)
                .setErrorMessage("Not SQLIdentifierExpr: " + ShardingUtil.debugSql(e, true))
                .bulid();
        }
        sqlFeature.setTable(tableNameLowerCase);
        needSharding = shardingRouter.isShardingTable(tableNameLowerCase);
        if (needSharding) {
            sqlFeature.setNeedSharding(true);
            composedKey = shardingRouter.getComposedKey(tableNameLowerCase);
            if (shardingRouter.isMappingShardingTable(tableNameLowerCase)) {
                mappingKeys.putAll(shardingRouter.getMappingKey(tableNameLowerCase));
            }
        }
    }

    protected static List<SQLStatement> parseMySQLStmt(String sql) {
        SQLStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> stmts = parser.parseStatementList();
        return stmts;
    }

    private static List<SQLStatement> parsePGSQLStmt(String sql) {
        SQLStatementParser parser = new DalPGSQLStatementParser(sql);
        // Druid对于带分号的SQL解析比较弱,所以PG目前只支持单条SQL的解析,不使用parseStatementList()
        List<SQLStatement> stmts = Collections.singletonList(parser.parseStatement());
        return stmts;
    }

    protected void rewriteTable(String table) {
        SQLExprTableSource source = new SQLExprTableSource();
        source.setExpr(new SQLIdentifierExpr(table));
        source.setAlias(tableNameAlias);
        if (sqlStmt instanceof SQLSelectQueryBlock) {
            source.setHints(((SQLSelectQueryBlock) sqlStmt).getFrom().getHints());
            ((SQLSelectQueryBlock) sqlStmt).setFrom(source);
        } else if (sqlStmt instanceof SQLUpdateStatement) {
            source.setHints(((SQLUpdateStatement) sqlStmt).getTableSource().getHints());
            ((SQLUpdateStatement) sqlStmt).setTableSource(source);
        } else if (sqlStmt instanceof SQLInsertStatement) {
            ((SQLInsertStatement) sqlStmt).setTableSource(source);
        } else if (sqlStmt instanceof SQLDeleteStatement) {
            ((SQLDeleteStatement) sqlStmt).setTableSource(source);
        }
    }

    /**
     * 返回第一维,第index张sharding表对应的ShardingTable
     *
     * @param index
     * @return
     * @throws QueryException 当指定index的ShardingTable不存在的时候
     */
    protected ShardingTable lookupShardingTableByShardingIndex(int index) {
        ShardingTable t = shardingRouter.getShardingTableByIndex(tableNameLowerCase, index);
        ensureShardingTableNotNull(t,
            () -> "ShardingTable of sharding_index = " + index + " not exist in sharding config");
        return t;
    }

    // 解析基本的语法,如SQL类型,表名等,需保证该方法不会抛出关于sharding的异常
    protected abstract void handleBasicSyntax();

    // 解析sharding相关的语法
    protected abstract void handleShardingSyntax();

    // 生成Sharding相关的SQL
    protected abstract void generateSQLs();

    /**
     * 获取sharding表的最后一片表名
     */
    protected List<ShardingTable> getLastShardingTable() {
        List<ShardingTable> allTables = shardingRouter.getAllShardingTable(tableNameLowerCase);
        if (allTables.isEmpty()) {
            throw new QueryException.Builder(ErrorCode.ERR_SQL_SYNTAX_ERROR).setErrorMessage(
                String.format("There is no sharding tables for [%s]", tableNameLowerCase)).bulid();
        }
        int tableSize = allTables.size();
        return Collections.singletonList(allTables.get(tableSize - 1));
    }

    public void collectShardingColumnValue(int shardingRuleIndex, String value4ShardingKey) {
        Objects.requireNonNull(composedKey, "composedKey is Null");
        String name4ShardingKey = composedKey.getShardingColumns().get(shardingRuleIndex);
        validShardingValues.shardingKeyNameWithValues
            .computeIfAbsent(name4ShardingKey, k -> new HashSet<>()).add(value4ShardingKey);
    }

    /**
     * 生成业务sql的sharding sql
     */
    public void generateOriginalShardedSQLs() {
    }

    /**
     * 前置条件:
     * 1. 原始sql需要sharding
     * 2. sql中where带了sharding_index = ? 条件
     */
    private void handleUpdateDeleteSelectForUpdateVirtualShardingIndex() {
        if (composedKey.getShardingColumns().size() != 1) {
            throw new QueryException.Builder(ErrorCode.ER_SYNTAX_ERROR).setErrorMessage(
                "cannot use sharding_index when sharding columns count = " + composedKey
                    .getShardingColumns().size()).bulid();
        }
        ShardingTable singleTable = lookupShardingTableByShardingIndex(whereHandler.shardingIndex);
        List<String> shardingIds = Collections.singletonList("sharding_index=" + singleTable.table);
        results = newShardingResult(Collections.singletonList(singleTable), shardingIds, 1);
    }

    /**
     * 根据原始sql的类型，以及参数生成合适类型的IterableShardingResult
     *
     * @param shardingTables    sharding分片
     * @param shardingIds       每个sharding分片对应的shardingId,依赖索引对应
     * @param shardingRuleCount sharding的维度,即sharding Rule的个数
     * @return IterableShardingResult
     */
    protected IterableShardingResult newShardingResult(List<ShardingTable> shardingTables,
        List<String> shardingIds, int shardingRuleCount) {
        return new IterableShardingResult(shardingTables);
    }

    /**
     * 检验传入的composekey values的合法性
     *
     * @param values
     */
    protected void isAllComposeKeyValuesShardingIdSame(Set<String> values) {
        if (values.size() == 1) {
            return;
        }
        HashMap<String, String> hashWithId = new HashMap<>();
        values.forEach(
            v -> hashWithId.put(shardingRouter.computeShardingId(tableNameLowerCase, v), v));
        if (hashWithId.size() != 1) {
            throw new QueryException.Builder(ErrorCode.ERR_SQL_SYNTAX_ERROR).setErrorMessage(
                queryType + " multi rows in different sharding id is not allowed, id->value: "
                    + hashWithId).bulid();
        }
    }

    /**
     * 检验传入的shardingkey values的合法性
     *
     * @param values
     */
    protected void isAllShardingKeyValuesShardingIdSame(Set<String> values) {
        if (values.size() == 1) {
            return;
        }
        HashMap<String, String> hashWithId = new HashMap<>();
        values.forEach(v -> hashWithId.put(shardingRouter.computeShardingId(v), v));
        if (hashWithId.size() != 1) {
            throw new QueryException.Builder(ErrorCode.ERR_SQL_SYNTAX_ERROR).setErrorMessage(
                queryType + " multi rows in different sharding id is not allowed, id->value: "
                    + hashWithId).bulid();
        }
    }

    /**
     * 前置条件:
     * 1. sql 类型是update,delete,select_for_update
     * 2. where 条件中存在有效的composeKey条件
     * 处理根据composeKey sharding的情况, 包含多composeKey的情况
     *
     * @param shardingKeys
     */
    private void handleShardingByComposeKey(List<String> shardingKeys) {
        Map.Entry<String, Set<String>> entry =
            validShardingValues.composeColumnNameWithValues.entrySet().iterator().next();
        String name4ComposeKey = entry.getKey();
        Set<String> values4ComposeKey = entry.getValue();
        isAllComposeKeyValuesShardingIdSame(values4ComposeKey);
        final String value4Sharding = values4ComposeKey.iterator().next();
        List<ShardingTable> tables = new ArrayList<>(2);
        List<String> shardingIds = new ArrayList<>(2);
        shardingKeys.forEach((column) -> {
            final ShardingTable t = shardingRouter
                .getShardingTableByComposedKey(tableNameLowerCase, column, value4Sharding);
            ensureShardingTableNotNull(t,
                () -> "ShardingTable == null of composed key = " + name4ComposeKey + " and value = "
                    + value4Sharding);
            String shardingId =
                shardingRouter.computeShardingId(tableNameLowerCase, value4Sharding);
            // 对于update/delete/select_for_update,因为只支持单sharding id sql,所以无需对composeKey在in条件导致的多个composeKey在同一个ShardingTable
            // 的情况进行去重操作
            tables.add(t);
            shardingIds.add(shardingId);
        });
        results = newShardingResult(tables, shardingIds, shardingKeys.size());
        if (shardingKeys.size() >= 2) {
            results.originShardingKey = Collections.singletonMap(entry.getKey(), entry.getValue());
        }
    }

    /**
     * 前置条件:
     * 1. sql 类型是update,delete,select_for_update
     * 2. where 条件中不存在有效的composeKey条件
     * 3. 存在有效的shardingKey及其值(值可以从原始sql中提取,也可以源自mapping表的查询得到)
     *
     * @param shardingKeys
     */
    private void handleShardingByShardingKey(List<String> shardingKeys) {
        List<ShardingTable> tables = new ArrayList<>(2);
        List<String> shardingIds = new ArrayList<>(2);
        final StringJoiner shardingId = new StringJoiner(SHARDING_ID_DELIMITER);
        for (String name4ShardingKey : shardingKeys) {
            Set<String> values4ShardingKey =
                validShardingValues.shardingKeyNameWithValues.get(name4ShardingKey);
            isAllShardingKeyValuesShardingIdSame(values4ShardingKey);
            final String value4Sharding = values4ShardingKey.iterator().next();
            final ShardingTable t = shardingRouter
                .getShardingTable(tableNameLowerCase, name4ShardingKey, value4Sharding);
            ensureShardingTableNotNull(t,
                () -> "ShardingTable == null of sharding key = " + name4ShardingKey
                    + " and value = " + value4Sharding);
            shardingId.add(shardingRouter.computeShardingId(value4Sharding));
            // 对于update/delete,因为只支持单sharding id sql,所以无需对composeKey在in条件导致的多个composeKey在同一个ShardingTable
            // 的情况进行去重操作
            tables.add(t);
        }
        tables.forEach(t -> shardingIds.add(shardingId.toString()));
        results = newShardingResult(tables, shardingIds, shardingKeys.size());
        if (shardingKeys.size() >= 2) {
            results.originShardingKey = validShardingValues.shardingKeyNameWithValues;
        }
    }

    /**
     * 前置条件:
     * 1. where条件中既不含合法的composekey,也不含合法的shardingKey,有配置mapping_key
     * 2. 一个mapping_key规则,且where 条件中包含至少一个合法的mappingkey表达式
     */
    private void handleShardingBySingleMappingKey(List<String> shardingKeys) {
        Map.Entry<String, MappingKey> entry = mappingKeys.entrySet().iterator().next();
        MappingKey mKey = entry.getValue();
        Optional<List<String>> name4MappingKeyOp =
            findFirstMatchedMappingName(mKey.getMappingColumns(),
                validShardingValues.mappingKeyNameWithValues.keySet());
        Objects.requireNonNull(name4MappingKeyOp.isPresent(),
            () -> "strange! can not find mapping key for sharding from "
                + validShardingValues.mappingKeyNameWithValues.keySet());
        final String composedMappingKey =
            name4MappingKeyOp.get().stream().collect(Collectors.joining(""));
        final String originMappingKey =
            name4MappingKeyOp.get().stream().collect(Collectors.joining(","));
        Set<String> values4MappingKey;
        //2列组合的mapping key场景
        if (name4MappingKeyOp.get().size() == 2) {
            values4MappingKey = computeComposedMappingKeyValue4Update(name4MappingKeyOp.get());
        } else {
            values4MappingKey =
                validShardingValues.mappingKeyNameWithValues.get(composedMappingKey);
        }

        // 此处隐含假定上述循环一定能找到一个mapping_rule用于sharding
        // 对于mapping_sharding，不对values做校验
        List<ShardingTable> tables = new ArrayList<>();
        List<SelectMappingEntity> selectMappingEntities = new ArrayList<>(1);

        final String mappingTablePrefix =
            mKey.getMappingRuleBySepName(originMappingKey).getTablePrefix();
        values4MappingKey.forEach(value -> {
            ShardingTable t = shardingRouter
                .getMappingTableByMappingKey(tableNameLowerCase, mKey, originMappingKey, value);
            ensureShardingTableNotNull(t, () -> String.format(
                "can not get ShardingTable when generate mapping sql, table=%s, columnName=%s, value=%s",
                tableNameAlias, originMappingKey, value));
            tables.add(t);
            SelectMappingEntity entity =
                new SelectMappingEntity(mappingTablePrefix, t, mKey.getColumn(), composedMappingKey,
                    value, 0);
            selectMappingEntities.add(entity);
        });
        List<String> shardingIds = Collections.nCopies(tables.size(), "");
        mappingResults =
            new UpdateDeleteSelectMappingShardingResult(tables, selectMappingEntities, shardingIds,
                shardingKeys.size());
        //清空mapping values,防止再次生成mapping sql
        validShardingValues.mappingKeyNameWithValues.clear();
    }

    /**
     * 从候选的键中找出在names中出现的第一个键
     *
     * @param names
     * @param candidates
     * @return
     */
    protected static Optional<String> findFirstMatchedName(List<String> names,
        Set<String> candidates) {
        return names.stream().filter(candidates::contains).findFirst();
    }

    protected static Optional<List<String>> findFirstMatchedMappingName(
        Map<String, List<String>> names, Set<String> candidates) {
        return names.values().stream().filter(candidates::containsAll).findFirst();
    }

    /**
     * 前置条件:
     * 1. 有2个mapping_key
     * 2. 解析到的mapping字段符合2个mapping_key中mapping_rule的要求
     * 3. 前面的sharding尝试都失败
     *
     * @param shardingKeys
     */
    private void handleShardingByTwoMappingKey(List<String> shardingKeys) {
        List<ShardingTable> tables = new ArrayList<>(2);
        List<SelectMappingEntity> selectMappingEntities = new ArrayList<>(2);
        // 只使用其计数功能，无需其原子性
        final AtomicInteger shardingRuleIndex = new AtomicInteger(0);
        mappingKeys.forEach((value4MappingKey, mKey) -> {
            Optional<List<String>> name4MappingKeyOp =
                findFirstMatchedMappingName(mKey.getMappingColumns(),
                    validShardingValues.mappingKeyNameWithValues.keySet());
            Objects.requireNonNull(name4MappingKeyOp.isPresent(),
                () -> "strange! can not find mapping key for sharding from "
                    + validShardingValues.mappingKeyNameWithValues.keySet());
            final String composedMappingKey =
                name4MappingKeyOp.get().stream().collect(Collectors.joining(""));
            final String originMappingKey =
                name4MappingKeyOp.get().stream().collect(Collectors.joining(","));
            Set<String> values4MappingKey;
            //2列组合的mapping key场景
            if (name4MappingKeyOp.get().size() == 2) {
                values4MappingKey = computeComposedMappingKeyValue4Update(name4MappingKeyOp.get());
            } else {
                values4MappingKey =
                    validShardingValues.mappingKeyNameWithValues.get(composedMappingKey);
            }
            // 此处隐含假定上述循环一定能找到一个mapping_rule用于sharding
            // 对于mapping_sharding，不对values做校验
            String mappingTablePrefix =
                mKey.getMappingRuleBySepName(originMappingKey).getTablePrefix();
            values4MappingKey.forEach(value -> {
                ShardingTable t = shardingRouter
                    .getMappingTableByMappingKey(tableNameLowerCase, mKey, originMappingKey, value);
                ensureShardingTableNotNull(t, () -> String.format(
                    "can not get ShardingTable when generate mapping sql, table=%s, columnName=%s, value=%s",
                    tableNameAlias, originMappingKey, value));
                SelectMappingEntity entity =
                    new SelectMappingEntity(mappingTablePrefix, t, mKey.getColumn(),
                        composedMappingKey, value, shardingRuleIndex.get());
                tables.add(t);
                selectMappingEntities.add(entity);
            });
            shardingRuleIndex.incrementAndGet();
        });
        // mapping sharded sql 跳过shardingId校验,所以赋值其shardingId为空
        List<String> shardingIds = Collections.nCopies(tables.size(), "");
        // UpdateDeleteMappingShardingResult需要更新,以支持2维mapping
        mappingResults =
            new UpdateDeleteSelectMappingShardingResult(tables, selectMappingEntities, shardingIds,
                shardingKeys.size());
        // 清空,防止再次生成mapping sql
        validShardingValues.mappingKeyNameWithValues.clear();
        // 清空从SQL中提取到的shardingKeyValues,在2维mapping的情况下,可能存在SQL中存在一维度的shardingKey,但是不足以用于更新操作.
        // 清空以避免对从mapping表中查到的shardingKeyValues造成干扰
        validShardingValues.shardingKeyNameWithValues.clear();
    }

    private Set<String> computeComposedMappingKeyValue4Update(List<String> name4MappingKey) {
        Set<String> firstMKeyVals =
            validShardingValues.mappingKeyNameWithValues.get(name4MappingKey.get(0));
        Set<String> secondMKeyVals =
            validShardingValues.mappingKeyNameWithValues.get(name4MappingKey.get(1));
        return firstMKeyVals.stream().map(firstMKeyVal -> {
            Set<String> result = new HashSet<>();
            secondMKeyVals.forEach(secondMKeyVal -> result.add(firstMKeyVal + "," + secondMKeyVal));
            return result;
        }).flatMap(Collection::stream).collect(Collectors.toSet());
    }

    private void handleShardingByLast(List<String> shardingKeys) {
        Map<String, List<ShardingTable>> shardingKeyWithTables =
            shardingRouter.getAllShardingTablesOfAllDimensions(tableNameLowerCase);
        List<ShardingTable> tables = new ArrayList<>(2);
        List<String> shardingIds = new ArrayList<>(2);
        StringJoiner shardingId = new StringJoiner(SHARDING_ID_DELIMITER);
        shardingKeys.forEach(name4ShardingKey -> {
            List<ShardingTable> shardingTables = shardingKeyWithTables.get(name4ShardingKey);
            ShardingTable t = shardingTables.get(shardingTables.size() - 1);
            tables.add(t);
            // 为此种情况赋予特殊的shardingId用于校验
            shardingId.add("last");
        });
        // 为由于从mapping表查询不到value,SQL发往最后一个shardingTable的场景赋予空shardingId以绕过shardingId校验
        tables.forEach(t -> shardingIds.add(shardingId.toString()));
        results = newShardingResult(tables, shardingIds, shardingKeys.size());
    }

    /**
     * 检测传入的targets是否在每个mapping_key的mapping_rules中都有配置
     *
     * @param mkeys
     * @param targets
     * @return
     */
    private static boolean isName4MappingKeyMatch(Collection<MappingKey> mkeys,
        Set<String> targets) {
        for (MappingKey mkey : mkeys) {
            Optional<List<String>> matchedKeyOp =
                mkey.getMappingColumns().values().stream().filter(targets::containsAll).findAny();
            if (!matchedKeyOp.isPresent()) {
                return false;
            }
        }
        return true;
    }

    protected void generateSQLsForUpdateDeleteSelectForUpdate() {
        if (!needSharding) {
            return;
        }
        if (whereHandler.shardingIndex != -1) {
            handleUpdateDeleteSelectForUpdateVirtualShardingIndex();
            return;
        }
        // 确保存在有效的sharding条件用于sharding,否则报错
        ensureHasValidShardingConditions();
        List<String> shardingKeys = composedKey.getShardingColumns();
        // 存在逻辑合法的composekey,则使用composekey sharding
        if (!validShardingValues.composeColumnNameWithValues.isEmpty()) {
            handleShardingByComposeKey(shardingKeys);
            return;
        }
        // 隐含前置条件: where条件中不含合法的composekey
        if (validShardingValues.shardingKeyNameWithValues.keySet().containsAll(shardingKeys)) {
            handleShardingByShardingKey(shardingKeys);
            return;
        }
        // 没有足够的shardingKey或不包含composeKey,且没有可以使用的mapping_key,则报错
        if (mappingKeys.isEmpty()) {
            throw new QueryException.Builder(ErrorCode.ERR_KEY_IS_NOT_COMPOSED).setErrorMessage(
                "this sql need to be sharded but ID ("
                    + validShardingValues.shardingKeyNameWithValues.keySet()
                    + ") is not composed key").bulid();
        }
        // 隐含前置条件: where条件中既不含合法的composekey,也不含合法的shardingKey,有配置mapping_key
        // 一个mapping_key规则,且where 条件中包含至少一个合法的mappingkey表达式
        if (mappingKeys.size() == 1 && !validShardingValues.mappingKeyNameWithValues.isEmpty()) {
            handleShardingBySingleMappingKey(shardingKeys);
            return;
        }
        // 隐含前置条件: where条件中既不含合法的composekey,也不含合法的shardingKey, 大于1维mapping_key
        // 2维mapping_key, 且每个mapping_key的key字段在where条件中存在合法的表达式
        if (mappingKeys.size() == 2 && isName4MappingKeyMatch(mappingKeys.values(),
            validShardingValues.mappingKeyNameWithValues.keySet())) {
            handleShardingByTwoMappingKey(shardingKeys);
            return;
        }
        // 通过mapping key没有查询到对应的value,将该sql默认发往最后一个分片
        // 二维mapping的情况，通过mapping key只查询到一维度的value,将该sql默认发往所有维度的最后一个分片
        if (!validShardingCondtion.mappingKeyNameWithSQLExpr.isEmpty()
            && validShardingValues.mappingKeyNameWithValues.isEmpty()
            && !validShardingValues.shardingKeyNameWithValues.keySet().containsAll(shardingKeys)) {
            handleShardingByLast(shardingKeys);
            return;
        }
        // 如果二维mapping的情况下,where条件中只带了一维度的shardingKey,则会走到此逻辑
        if (!validShardingValues.shardingKeyNameWithValues.isEmpty()) {
            throw new QueryException.Builder(ErrorCode.ERR_KEY_IS_NOT_MAPPING_KEY).setErrorMessage(
                "this sql need to be sharded but " + validShardingValues.shardingKeyNameWithValues
                    .keySet() + " is not mapping key").bulid();
        }
        // 兜底情况，不应该执行的代码，如果走到此逻辑,则认为出现了未考虑到的逻辑,或者修改的代码出现了bug
        throw new IllegalStateException(
            "strange! reach unreachable method generateSQLsForUpdateDeleteSelectForUpdate");
    }

    protected void ensureWhereNotNull(SQLExpr where, String errorMsg) {
        if (where == null) {
            needSharding = false;
            throw new QueryException.Builder(ErrorCode.ERR_SQL_SYNTAX_ERROR)
                .setErrorMessage(errorMsg).bulid();
        }
    }

    /**
     * 检验传入的ShardingTable是否为Null, 如果为Null则依据指定的ErrorCode, errorMsg构造QueryException并抛出异常, 否则什么都不做
     *
     * @param shardingTable    待检验的shardingTable
     * @param errorMsgSupplier 抛异常时使用的错误消息Supplier
     * @throws QueryException if shardingTable == null
     */
    protected static void ensureShardingTableNotNull(final ShardingTable shardingTable,
        final Supplier<String> errorMsgSupplier) {
        if (Objects.isNull(shardingTable)) {
            throw new QueryException.Builder(ErrorCode.ERR_KEY_CANNOT_FOUND_IN_CFG)
                .setErrorMessage(errorMsgSupplier.get()).bulid();
        }
    }

    private void ensureHasValidShardingConditions() {
        // 存在任意有效的composeKey则提前返回
        if (!validShardingCondtion.composeColumnNameWithSQLExpr.isEmpty()) {
            return;
        }
        // 存在任意有效的shardingKey则提前返回
        if (!validShardingCondtion.shardingKeyNameWithSQLExpr.isEmpty()) {
            return;
        }
        // 存在任意有效的mappingKey则提前返回
        if (!validShardingCondtion.mappingKeyNameWithSQLExpr.isEmpty()) {
            return;
        }
        // 构造报错信息
        StringBuilder sb = new StringBuilder();
        if (Objects.nonNull(composedKey)) {
            if (!composedKey.getColumn().isEmpty()) {
                sb.append("composeKey = ").append(composedKey.getColumn());
            }
            sb.append(", shardingKey = ").append(composedKey.getShardingColumns());
        }
        if (!mappingKeys.isEmpty()) {
            mappingKeys.forEach((col, mKey) -> {
                sb.append(", mappingKey = ").append(mKey.getMappingColumns());
            });
        }
        throw new QueryException.Builder(ErrorCode.ERR_NO_COMPOSED_ID).setErrorMessage(
            "this sql need to be sharded but can not extract any key for sharding : " + sb).bulid();
    }

    public class UpdateDeleteShardingResult extends IterableShardingResult {
        private OwnerWhereExprHandler handler;
        // 每个ShardingResult的shardingId,顺序与ShardingTable一致
        private final List<String> shardingIds;

        public UpdateDeleteShardingResult(List<ShardingTable> shardingTables,
            List<String> shardingIds, int shardingRuleCount) {
            super(shardingTables);
            this.shardingIds = shardingIds;
            this.shardingRuleCount = shardingRuleCount;
            handler = new OwnerWhereExprHandler(tableNameLowerCase, literalSymbol);
            if (Objects.nonNull(where)) {
                handler.parseWhere(where);
            }
        }


        @Override public ShardingResult next() {
            if (index >= shardingTables.size()) {
                throw new NoSuchElementException();
            }
            ShardingTable t = shardingTables.get(index);
            String table = t.table;
            String database = t.database;
            if (Objects.nonNull(where) && Objects.isNull(tableNameAlias)) {
                if (ShardingSQL.this instanceof ShardingUpdateSQL) {
                    handler.rewriteWhereOwners(table);
                    ((ShardingUpdateSQL) ShardingSQL.this)
                        .rewriteOwnerInSETExpr(((SQLUpdateStatement) sqlStmt).getItems(), table);
                } else if (ShardingSQL.this instanceof ShardingDeleteSQL) {
                    handler.rewriteWhereOwners(table);
                }
            }
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


    protected class SelectMappingEntity {

        /**
         * 所使用的mapping表前缀
         */
        public final String mappingTablePrefix;
        public final ShardingTable shardingTable;
        public final String name4MappingValue;
        public final String name4MappingKey;
        public final String value4MappingKey;
        /**
         * 多维sharding时,用于标识关联到哪一维度上
         */
        public final int shardingRuleIndex;

        public SelectMappingEntity(String mappingTablePrefix, ShardingTable shardingTable,
            String name4MappingValue, String name4MappingKey, String value4MappingKey,
            int shardingRuleIndex) {
            this.mappingTablePrefix = mappingTablePrefix;
            this.shardingTable = shardingTable;
            this.name4MappingValue = name4MappingValue;
            this.name4MappingKey = name4MappingKey;
            this.value4MappingKey = value4MappingKey;
            this.shardingRuleIndex = shardingRuleIndex;
        }
    }


    protected class UpdateDeleteSelectMappingShardingResult extends IterableShardingResult {
        private static final String SELECT_MAPPING_SQL_TEMPLATE = "SELECT %s FROM %s WHERE %s = %s";
        private static final String SQL_VARCHAR_TEMPLATE = "'%s'";
        private static final String WHITE_FIELDS_TEMPLATE = "%s = %s";

        private final List<SelectMappingEntity> entities;
        private final List<String> shardingIds;

        public UpdateDeleteSelectMappingShardingResult(List<ShardingTable> shardingTables,
            List<SelectMappingEntity> entities, List<String> shardingIds, int shardingRuleCount) {
            super(shardingTables);
            this.entities = entities;
            this.shardingIds = shardingIds;
            this.shardingRuleCount = shardingRuleCount;
        }

        private void rewriteWhiteFields(String key, String value) {
            whiteFields = String.format(WHITE_FIELDS_TEMPLATE, key, value);
        }

        private void rewriteMostSafeSQL(String value4MappingKey, String mappingTablePrefix,
            String name4MappingKey) {
            mostSafeSQL = String
                .format(SELECT_MAPPING_SQL_TEMPLATE, value4MappingKey, mappingTablePrefix,
                    name4MappingKey, "?");
        }

        @Override public ShardingResult next() {
            if (index >= shardingTables.size()) {
                throw new NoSuchElementException();
            }
            ShardingTable t = shardingTables.get(index);
            SelectMappingEntity entity = entities.get(index);
            String mappingValue = String.format(SQL_VARCHAR_TEMPLATE, entity.value4MappingKey);
            String shardedSql = String
                .format(SELECT_MAPPING_SQL_TEMPLATE, entity.name4MappingValue, t.table,
                    entity.name4MappingKey, mappingValue);
            ShardingResult r = new ShardingResult(shardedSql, t.table, t.database);
            rewriteMostSafeSQL(entity.name4MappingValue, entity.mappingTablePrefix,
                entity.name4MappingKey);
            rewriteWhiteFields(entity.name4MappingKey, mappingValue);
            r.id = shardingIds.get(index);
            r.shardingRuleIndex = entity.shardingRuleIndex;
            r.shardingRuleCount = shardingRuleCount;
            r.queryType = QUERY_TYPE.SELECT;
            currentResult = r;
            index++;
            return r;
        }
    }

    public static String addPrefixComment(String queryComment, String sql) {
        if (queryComment == null || queryComment.isEmpty()) {
            return sql;
        } else {
            return queryComment + sql;
        }
    }

    private static ShardingSQL createShardingSQL(List<SQLStatement> stmtList,
        ShardingRouter shardingRouter, Send2BatchCond batchCond) {
        SQLStatement stmt = stmtList.get(0);
        if (stmtList.size() > 1) {
            return new ShardingMultiQuery(stmt, stmtList, shardingRouter);
        } else if (stmt instanceof SQLSelectStatement) {
            if (stmt instanceof PGSelectStatement) {
                return new PGShardingSelectSQL(stmt, shardingRouter);
            } else {
                return new ShardingSelectSQL(stmt, shardingRouter);
            }
        } else if (stmt instanceof SQLUpdateStatement) {
            return new ShardingUpdateSQL(stmt, shardingRouter);
        } else if (stmt instanceof SQLInsertStatement) {
            return new ShardingInsertSQL(stmt, shardingRouter);
        } else if (stmt instanceof SQLDeleteStatement) {
            return new ShardingDeleteSQL(stmt, shardingRouter);
        } else {
            if (batchCond.isBatchAllowed() && stmt instanceof SQLTruncateStatement) {
                return new ShardingTruncateSQL(stmt, shardingRouter);
            }
            return new ShardingOtherSQL(stmt, shardingRouter);
        }
    }

    private static ShardingSQL handleSQLBasicly(String sql, String queryComment,
        ShardingRouter shardingRouter, Function<String, List<SQLStatement>> f,
        Send2BatchCond batchCond, Predicate<String> whiteFieldsFilter) {
        ShardingSQL shardingSQL = null;
        List<SQLStatement> stmtList = null;
        try {
            stmtList = f.apply(sql);
        } catch (Exception e) {
            // when this sql syntax is invalid
            String msg = String.format(Constants.INVALID_SQL_SYNTAX, sql, e.getMessage());
            throw new QueryException.Builder(ErrorCode.ER_SYNTAX_ERROR).setSequenceId(1)
                .setErrorMessage(msg).bulid(e);
        }
        shardingSQL = createShardingSQL(stmtList, shardingRouter, batchCond);
        shardingSQL.send2Batch = batchCond;
        shardingSQL.whiteFieldsFilter = whiteFieldsFilter;
        handleSQLConstruct(shardingSQL, sql, queryComment, stmtList);
        return shardingSQL;
    }

    public static void handleSQLConstruct(ShardingSQL shardingSQL, String sql, String queryComment,
        List<SQLStatement> stmtList) {
        try {
            shardingSQL.comment = queryComment;
            shardingSQL.handleBasicSyntax();
            SafeSQL safeSql =
                ShardingUtil.mostSafeSQLAndWhiteFields(stmtList, shardingSQL.whiteFieldsFilter);
            shardingSQL.setMostSafeSQL(safeSql.mostSafeSql);
            shardingSQL.setOriginMostSafeSQL(safeSql.mostSafeSql);
            shardingSQL.setWhiteFields(safeSql.whiteFields);
            shardingSQL.setOriginWhiteFields(safeSql.whiteFields);
            if (!shardingSQL.needRewriteSQLWhenNoSharding()) {
                shardingSQL.originSQL = sql;
            }
            // 如果SQL类型是SET_AUTOCOMMIT, 则将SQL PATTERN变为SET AUTOCOMMIT = 0,SET AUTOCOMMIT = 1的样式
            if (Objects.equals(shardingSQL.queryType, QUERY_TYPE.SET_AUTOCOMMIT)) {
                shardingSQL.setMostSafeSQL(shardingSQL.originSQL);
                shardingSQL.setOriginMostSafeSQL(shardingSQL.originSQL);
            }
        } catch (QueryException e) {
            String msg = String.format(Constants.INVALID_SHARDING_SQL, sql, e.errorMessage);
            throw new QueryException.Builder(e.errorCode).setSequenceId(1).setErrorMessage(msg)
                .bulid(e);
        } catch (Exception e) {
            String msg = String.format(Constants.INVALID_SHARDING_SQL, sql, e.getMessage());
            throw new QueryException.Builder(ErrorCode.ERR).setErrorMessage(msg).bulid(e);
        }
    }

    public static ShardingSQL handleMySQLBasicly(String sql, String queryComment,
        ShardingRouter shardingRouter, Send2BatchCond batchCond,
        Predicate<String> whiteFieldsFilter) {
        return handleSQLBasicly(sql, queryComment, shardingRouter,
            (querySQL) -> parseMySQLStmt(querySQL), batchCond, whiteFieldsFilter);
    }

    public static ShardingSQL handlePGBasicly(String sql, String queryComment,
        ShardingRouter shardingRouter, Send2BatchCond batchCond,
        Predicate<String> whiteFieldsFilter) {
        return handleSQLBasicly(sql, queryComment, shardingRouter,
            (querySQL) -> parsePGSQLStmt(querySQL), batchCond, whiteFieldsFilter);
    }

    public static void handleSQLSharding(ShardingSQL shardingSQL) {
        try {
            shardingSQL.handleShardingSyntax();
            shardingSQL.generateSQLs();
        } catch (QueryException e) {
            String msg = String
                .format(Constants.INVALID_SHARDING_SQL, shardingSQL.originSQL, e.errorMessage);
            throw new QueryException.Builder(e.errorCode).setSequenceId(1).setErrorMessage(msg)
                .bulid(e);
        } catch (Exception e) {
            String msg = String
                .format(Constants.INVALID_SHARDING_SQL, shardingSQL.originSQL, e.getMessage());
            throw new QueryException.Builder(ErrorCode.ERR).setSequenceId(1).setErrorMessage(msg)
                .bulid(e);
        }
    }

    /**
     * 在非sharding模式下,该条SQL(originSQL和safeOriginalSQL)是否会被重写
     * 该方法会在某些情况下被子类重写getMappingResults
     * 如遇到SHOW FULL TABLES FROM `dalgroup`语句
     * 则需要替换里面dalgroup为真正的dbname
     * 详细可参见 @see ShardingOtherSQL
     *
     * @return false表示使用默认
     * <p>
     * true子类会自行设置这些值
     */
    public boolean needRewriteSQLWhenNoSharding() {
        return false;
    }

    public void setMostSafeSQL(String mostSafeSQL) {
        this.mostSafeSQL = cutIfSqlTooLong(mostSafeSQL);
    }

    public String getMostSafeSQL() {
        return mostSafeSQL;
    }

    public String getOriginMostSafeSQL() {
        return originMostSafeSQL;
    }

    public void setOriginMostSafeSQL(String originMostSafeSQL) {
        this.originMostSafeSQL = cutIfSqlTooLong(originMostSafeSQL);
    }

    /**
     * 对sql 长度大于 maxSqlPatternLength 2倍的sql pattern, 截除掉超出部分
     *
     * @param sql
     * @return
     */
    private String cutIfSqlTooLong(String sql) {
        if (sql.length() > GreySwitch.getInstance().getMaxSqlPatternTruncLength()) {
            sql = sql.substring(0, GreySwitch.getInstance().getMaxSqlPatternTruncLength());
        }
        return sql;
    }

    public IterableShardingResult getMappingResults() {
        return mappingResults;
    }

    public void resetShardingResult() {
        results = IterableShardingResult.EMPTY_ITERABLE_SHARDING_RESULT;
        mappingResults = IterableShardingResult.EMPTY_ITERABLE_SHARDING_RESULT;
    }

    public String getWhiteFields() {
        return whiteFields;
    }

    public void setWhiteFields(String whiteFields) {
        this.whiteFields = whiteFields;
    }

    public String getOriginWhiteFields() {
        return originWhiteFields;
    }

    public void setOriginWhiteFields(String originWhiteFields) {
        this.originWhiteFields = originWhiteFields;
    }

    /**
     * rebalance时修改语法树节点。
     * 目前只有ShardingUpdateSQL有实现，其它类型SQL都是空实现
     *
     * @param reshardAt
     */
    public void rewriteSQL4ReBalance(String reshardAt) {
    }

    /**
     * 在SQL中注入index hints, 目前只支持SELECT, SELECT_FOR_UPDATE类型SQL.
     *
     * @param hints
     * @return true 实际改写了SQL, false 没有实际改写SQL
     */
    public boolean rewriteSQLHints(List<SQLHint> hints) {
        return false;
    }

}
