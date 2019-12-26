package me.ele.jarch.athena.sharding.sql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.*;
import com.alibaba.druid.sql.dialect.postgresql.ast.stmt.PGSQLStatement;
import com.alibaba.druid.sql.dialect.postgresql.ast.stmt.PGShowStatement;
import com.github.mpjct.jmpjct.mysql.proto.Column;
import com.github.mpjct.jmpjct.mysql.proto.ResultSet;
import com.github.mpjct.jmpjct.util.ErrorCode;
import me.ele.jarch.athena.allinone.DBRole;
import me.ele.jarch.athena.exception.QueryException;
import me.ele.jarch.athena.scheduler.DBChannelDispatcher;
import me.ele.jarch.athena.sharding.ShardingRouter;
import me.ele.jarch.athena.sharding.ShardingTable;
import me.ele.jarch.athena.sharding.showdb.DalShowStatementRewriter;
import me.ele.jarch.athena.sql.QUERY_TYPE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;


public class ShardingOtherSQL extends ShardingSQL {
    private static final Logger logger = LoggerFactory.getLogger(ShardingOtherSQL.class);

    public static final byte[] SHOW_WARNINGS_PACKET;

    static {
        /**@formatter:off
         * http://dev.mysql.com/doc/refman/5.6/en/show-warnings.html
         *
         * mysql> SHOW WARNINGS;
         * +---------+------+----------------------------------------+
         * | Level   | Code | Message                                |
         * +---------+------+----------------------------------------+
         * | Warning | xxx | xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx |
         * +---------+------+----------------------------------------+
         * 1 row in set (0.00 sec)
         * @formatter:on
         */
        ResultSet rs = new ResultSet();
        rs.addColumn(new Column("Level"));
        rs.addColumn(new Column("Code"));
        rs.addColumn(new Column("Message"));
        ArrayList<byte[]> data = rs.toPackets();
        int size = data.stream().mapToInt(x -> x.length).sum();
        byte[] rt = new byte[size];
        int pos = 0;
        for (byte[] bytes : data) {
            System.arraycopy(bytes, 0, rt, pos, bytes.length);
            pos += bytes.length;
        }
        SHOW_WARNINGS_PACKET = rt;
    }

    private boolean needWriteSQLWhenNoSharding = false;

    public ShardingOtherSQL(SQLObject sqlStmt, ShardingRouter shardingRouter) {
        super(sqlStmt, shardingRouter);
    }

    private static boolean isIgnoreStatement(SQLObject o) {
        if (o instanceof SQLUseStatement) {
            logger.error(ShardingUtil.debugSql(o) + " found, it's danger");
            return true;
        }
        return o instanceof MySqlSetTransactionStatement;
    }

    private void assignPGSQLType() {
        if (sqlStmt instanceof PGShowStatement)
            queryType = QUERY_TYPE.SHOW_CMD;
    }

    @Override protected void handleBasicSyntax() {
        if (sqlStmt instanceof PGSQLStatement) {
            assignPGSQLType();
            return;
        }
        if (sqlStmt instanceof SQLCommitStatement) {
            queryType = QUERY_TYPE.COMMIT;
        } else if (sqlStmt instanceof SQLRollbackStatement) {
            SQLRollbackStatement rollBackStmt = (SQLRollbackStatement) sqlStmt;
            if (Objects.nonNull(rollBackStmt.getTo())) {
                queryType = QUERY_TYPE.ROLLBACK_TO;
            } else {
                queryType = QUERY_TYPE.ROLLBACK;
            }
        } else if (sqlStmt instanceof SQLSavePointStatement) {
            queryType = QUERY_TYPE.SAVEPOINT;
        } else if (sqlStmt instanceof SQLReleaseSavePointStatement) {
            queryType = QUERY_TYPE.RELEASE;
        } else if (sqlStmt instanceof MySqlShowWarningsStatement) {
            queryType = QUERY_TYPE.SHOW_WARNINGS;
        } else if (sqlStmt instanceof MySqlShowVariantsStatement
            || sqlStmt instanceof MySqlShowCollationStatement) {
            queryType = QUERY_TYPE.SHOW_CMD;
            needCacheResult = true;
        } else if (sqlStmt instanceof SQLStartTransactionStatement) {
            queryType = QUERY_TYPE.BEGIN;
        } else if (isIgnoreStatement(sqlStmt)) {
            queryType = QUERY_TYPE.IGNORE_CMD;
        } else if (sqlStmt instanceof MySqlShowStatement
            || sqlStmt instanceof SQLShowTablesStatement) {
            queryType = QUERY_TYPE.SHOW_CMD;
            handleShowStatement();
        } else if (sqlStmt instanceof SQLReplaceStatement) {
            queryType = QUERY_TYPE.REPLACE;
        } else if (sqlStmt instanceof SQLSetStatement) {
            queryType = QUERY_TYPE.IGNORE_CMD;
            SQLSetStatement setStmt = (SQLSetStatement) sqlStmt;
            if (setStmt.getItems().size() != 1) {
                return;
            }
            SQLAssignItem item = setStmt.getItems().get(0);
            String target, value;
            if (item.getTarget() == null || item.getValue() == null) {
                return;
            }
            target = item.getTarget().toString().toLowerCase();
            value = item.getValue().toString();
            if (target.equals("autocommit") || target.equals("@@autocommit")) {
                queryType = QUERY_TYPE.SET_AUTOCOMMIT;
                if (value.equals("0") || value.equalsIgnoreCase("off")) {
                    autocommitValue = 0;
                } else if (value.equals("1") || value.equalsIgnoreCase("on")) {
                    autocommitValue = 1;
                } else {
                    throw new QueryException.Builder(ErrorCode.ER_SYNTAX_ERROR)
                        .setErrorMessage("invalid set autocommit").bulid();
                }
            }
            // autocommit不脱敏,且重写原来的SQL
            this.originSQL = ShardingUtil.debugSql(setStmt);
            this.needWriteSQLWhenNoSharding = true;
        } else if (sqlStmt instanceof MySqlKillStatement) {
            queryType = QUERY_TYPE.KILL_CMD;
            MySqlKillStatement killStmt = (MySqlKillStatement) sqlStmt;
            try {
                clientIdForKill = ShardingUtil.getNumberVal(killStmt.getThreadId()).longValue();
            } catch (Exception e) {
                logger.error("failed to parse kill command: " + ShardingUtil.debugSql(killStmt), e);
            }
        } else {
            logger.error("Other SQL Type: " + ShardingUtil.debugSql(sqlStmt, true));
        }
    }

    @Override protected void handleShardingSyntax() {
    }

    @Override protected void generateSQLs() {

    }

    // 目前主要支持以下几种情况
    // 1. [非sharding][需要回归测试]show create table dim_city
    // 2. [sharding][新加功能]show create table tb_shipping_order_100
    // 2.1 [sharding][与原先行为不同,需回归测试]show create table tb_shipping_order
    // 原先会发往home库,现在行为是发往最后一个库,并使用最后一张表的结果,如show create table tb_shipping_order_511
    // 3. [非sharding][需要回归测试]show columns from alpha_apollo_sharding_group.dim_city
    // 4. [sharding][新加功能]show columns from alpha_apollo_sharding_group.tb_shipping_order_100
    // 5. [sharding][与原先行为不同,需回归测试]show columns from tb_shipping_order
    // 原先会发往home库,现在行为是发往最后一个库,并使用最后一张表的结果,如show columns from tb_shipping_order_511
    // 6. [非sharding][需要回归测试] show tables from alpha_apollo_sharding_group转换为show tables from `apollo`发往主库
    // 7. [sharding][新加功能] show tables from alpha_apollo_sharding_group1转换为show tables直接发往相应的库
    // 8. 其他一些show命令
    private void handleShowStatement() {
        // 1,2:单独处理show create table命令(sharding的表),因为其不用重写db名字
        if (convertShowCreateShardingTables()) {
            return;
        }
        String dalGroup = null;
        DalShowStatementRewriter showRewriter =
            DalShowStatementRewriter.createDalShowStatementRewriter(sqlStmt);
        if (Objects.isNull(showRewriter)) {
            return;
        }
        if (showRewriter.getShowDBName() instanceof SQLName) {
            dalGroup = ((SQLName) showRewriter.getShowDBName()).getSimpleName();
        } else if (showRewriter.getShowDBName() instanceof SQLExpr) {
            dalGroup = ShardingUtil.getVal(showRewriter.getShowDBName());
        }
        if (dalGroup == null) {
            // 5.处理没有group名字,但是表是sharding表的类似于show columns from tb_shipping_order情况
            handleShardingShow(showRewriter);
            return;
        }
        dalGroup = ShardingUtil.removeLiteralSymbol(dalGroup, literalSymbol);

        // 3,4: sharding模式的show命令,会替换表名,并且找到合适的分库,
        //仅当show columns, indexes, index, keys时
        if (handleShardingShow(showRewriter)) {
            return;
        }

        // 6: 通过show tables from <group>中的group找到一个对应的Scheduler,获取真实数据库名字并替换
        String dbName = getDbinfoByDalGroup(dalGroup);
        if (Objects.nonNull(dbName)) {
            // 发往DB的SQL是重写过的真实数据库的名字
            showRewriter.rewriteDBName(String.format("`%s`", dbName));
            this.originSQL = ShardingUtil.debugSql(sqlStmt);
            // 设置flag,该sql将被重写
            this.needWriteSQLWhenNoSharding = true;
        } else {
            // 7: 支持show tables from alpha_apollo_sharding_group1命令,发往show tables命令相应的db,但是去除 from <db>
            handleShowTablesByShardingGroup(dalGroup);
        }
    }

    private void handleShowTablesByShardingGroup(String dalGroup) {
        if (!(sqlStmt instanceof SQLShowTablesStatement && shardingRouter.getAllShardingDbGroups()
            .contains(dalGroup))) {
            return;
        }

        SQLShowTablesStatement oldShowTables = (SQLShowTablesStatement) sqlStmt;
        oldShowTables.setDatabase(null);
        needSharding = true;
        String shardedSQL = ShardingUtil.debugSql(oldShowTables);
        // 为什么日志里会打印SHOW TABLES FROM alpha_apollo_sharding_group1
        ShardingResult result = new ShardingResult(shardedSQL, "showtables_cmd_no_table", dalGroup);
        result.queryType = QUERY_TYPE.SHOW_CMD;
        results = new SimpleIterableShardingResult(Arrays.asList(result));
    }

    private boolean convertShowCreateShardingTables() {
        if (!(sqlStmt instanceof MySqlShowCreateTableStatement)) {
            return false;
        }
        MySqlShowCreateTableStatement showCreateTable = (MySqlShowCreateTableStatement) sqlStmt;
        String table = ShardingUtil.debugSql(showCreateTable.getName());
        ShardingTable shardingTable = shardingRouter.getAllShardedTables().get(table);
        if (shardingTable == null) {
            return false;
        }
        showCreateTable.setName(new SQLIdentifierExpr(shardingTable.table));
        needSharding = true;
        String shardedSQL = ShardingUtil.debugSql(showCreateTable);
        ShardingResult result =
            new ShardingResult(shardedSQL, shardingTable.table, shardingTable.database);
        result.queryType = QUERY_TYPE.SHOW_CMD;
        results = new SimpleIterableShardingResult(Arrays.asList(result));
        return true;
    }

    private boolean handleShardingShow(DalShowStatementRewriter showRewriter) {
        String table = showRewriter.getTableIfExist();
        if (Objects.isNull(table)) {
            return false;
        }
        ShardingTable shardingTable = shardingRouter.getAllShardedTables().get(table);
        if (Objects.isNull(shardingTable)) {
            return false;
        }

        needSharding = true;
        showRewriter.rewriteTableIfExist(shardingTable.table);
        showRewriter.removeDB();
        String shardedSQL = ShardingUtil.debugSql(sqlStmt);
        ShardingResult result =
            new ShardingResult(shardedSQL, shardingTable.table, shardingTable.database);
        result.queryType = QUERY_TYPE.SHOW_CMD;
        results = new SimpleIterableShardingResult(Arrays.asList(result));
        return true;
    }

    // 传入dalgroup的名字,返回这个dalgroup对应的dbgroup下的数据库名字
    public String getDbinfoByDalGroup(String dalgroup) {
        // 通过group名字找到DBChannelDispatcher
        DBChannelDispatcher dispatcher = DBChannelDispatcher.getHolders().get(dalgroup);
        if (Objects.isNull(dispatcher)) {
            return null;
        }
        String tbFoundDB = dispatcher.getDalGroup().getDbGroup();
        try {
            String dbId = dispatcher.getDbGroup(tbFoundDB).getDBServer(DBRole.SLAVE);
            return dispatcher.getSched(dbId).getInfo().getDatabase();
        } catch (Exception e) {
            logger.error("failed to get dabase name by dbGroup:" + tbFoundDB, e);
        }
        return null;
    }

    @Override public boolean needRewriteSQLWhenNoSharding() {
        return needWriteSQLWhenNoSharding;
    }
}
