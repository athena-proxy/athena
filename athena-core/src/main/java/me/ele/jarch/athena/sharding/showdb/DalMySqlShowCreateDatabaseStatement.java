package me.ele.jarch.athena.sharding.showdb;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateDatabaseStatement;

public class DalMySqlShowCreateDatabaseStatement extends DalShowStatementRewriter {
    private MySqlShowCreateDatabaseStatement sqlStmt = null;

    @Override public SQLExpr getShowDBName() {
        return sqlStmt.getDatabase();
    }

    @Override public void rewriteDBName(String dbName) {
        sqlStmt.setDatabase(new SQLIdentifierExpr(dbName));
    }

    @Override public void setSqlStmt(SQLObject e) {
        this.sqlStmt = (MySqlShowCreateDatabaseStatement) e;
    }
}
