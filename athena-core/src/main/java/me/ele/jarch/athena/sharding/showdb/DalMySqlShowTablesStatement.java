package me.ele.jarch.athena.sharding.showdb;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLShowTablesStatement;

public class DalMySqlShowTablesStatement extends DalShowStatementRewriter {
    private SQLShowTablesStatement sqlStmt = null;

    @Override public SQLExpr getShowDBName() {
        return sqlStmt.getDatabase();
    }

    @Override public void rewriteDBName(String dbName) {
        sqlStmt.setDatabase(new SQLIdentifierExpr(dbName));
    }

    @Override public void setSqlStmt(SQLObject e) {
        this.sqlStmt = (SQLShowTablesStatement) e;
    }

}
