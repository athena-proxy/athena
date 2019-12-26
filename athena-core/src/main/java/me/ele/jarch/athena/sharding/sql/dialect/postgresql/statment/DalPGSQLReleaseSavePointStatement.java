package me.ele.jarch.athena.sharding.sql.dialect.postgresql.statment;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.statement.SQLReleaseSavePointStatement;

public class DalPGSQLReleaseSavePointStatement extends SQLReleaseSavePointStatement {

    private SQLExpr savepoint;

    public DalPGSQLReleaseSavePointStatement() {
    }

    public DalPGSQLReleaseSavePointStatement(String dbType) {
        super(dbType);
    }

    public void setSavepoint(SQLExpr savepoint) {
        this.savepoint = savepoint;
    }

    public SQLExpr getSavepoint() {
        return savepoint;
    }
}
