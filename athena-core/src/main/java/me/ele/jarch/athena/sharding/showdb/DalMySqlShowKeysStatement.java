package me.ele.jarch.athena.sharding.showdb;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowKeysStatement;
import me.ele.jarch.athena.sharding.sql.ShardingUtil;

import java.util.Objects;

public class DalMySqlShowKeysStatement extends DalShowStatementRewriter {
    private MySqlShowKeysStatement sqlStmt = null;
    private String table = null;

    @Override public SQLExpr getShowDBName() {
        return sqlStmt.getDatabase();
    }

    @Override public void rewriteDBName(String dbName) {
        sqlStmt.setDatabase(new SQLIdentifierExpr(dbName));
    }

    @Override public void removeDB() {
        sqlStmt.setDatabase(null);
    }

    @Override public void setSqlStmt(SQLObject e) {
        this.sqlStmt = (MySqlShowKeysStatement) e;
    }

    @Override public String getTableIfExist() {
        if (Objects.isNull(sqlStmt.getTable())) {
            return null;
        }
        if (table == null) {
            table =
                ShardingUtil.removeLiteralSymbol(sqlStmt.getTable().getSimpleName(), literalSymbol);
        }
        return table;
    }

    @Override public void rewriteTableIfExist(String table) {
        this.sqlStmt.setTable(new SQLIdentifierExpr(table));
    }

}
