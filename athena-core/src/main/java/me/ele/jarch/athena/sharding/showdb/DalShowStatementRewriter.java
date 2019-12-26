package me.ele.jarch.athena.sharding.showdb;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.statement.SQLShowTablesStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DalShowStatementRewriter {
    private static final Logger logger = LoggerFactory.getLogger(DalShowStatementRewriter.class);

    /**
     * 此类及其子类只为MySQL语法服务,所以直接写死为反引号。
     */
    protected final char literalSymbol = '`';

    public String getTableIfExist() {
        return null;
    }

    public void rewriteTableIfExist(String table) {
    }

    public abstract SQLExpr getShowDBName();

    public abstract void rewriteDBName(String dbName);

    public abstract void setSqlStmt(SQLObject sqlStmt);

    /**
     * remove the db field in the SQL AST
     */
    public void removeDB() {
        logger.error("removeDB is not implemented by " + this);
    }

    // https://dev.mysql.com/doc/refman/5.6/en/show.html
    // SHOW [FULL] COLUMNS FROM tbl_name [FROM db_name] [like_or_where]
    // SHOW CREATE DATABASE db_name
    // SHOW INDEX FROM tbl_name [FROM db_name]
    // SHOW OPEN TABLES [FROM db_name] [like_or_where]
    // SHOW TABLE STATUS [FROM db_name] [like_or_where]
    // SHOW [FULL] TABLES [FROM db_name] [like_or_where]
    // SHOW TRIGGERS [FROM db_name] [like_or_where]
    public static DalShowStatementRewriter createDalShowStatementRewriter(SQLObject sqlStmt) {
        DalShowStatementRewriter rewriter = null;
        if (sqlStmt instanceof SQLShowTablesStatement) {
            rewriter = new DalMySqlShowTablesStatement();
        } else if (sqlStmt instanceof MySqlShowTableStatusStatement) {
            rewriter = new DalMySqlShowTableStatusStatement();
        } else if (sqlStmt instanceof MySqlShowColumnsStatement) {
            rewriter = new DalMySqlShowColumnsStatement();
        } else if (sqlStmt instanceof MySqlShowCreateDatabaseStatement) {
            rewriter = new DalMySqlShowCreateDatabaseStatement();
        } else if (sqlStmt instanceof MySqlShowIndexesStatement) {
            rewriter = new DalMySqlShowIndexesStatement();
        } else if (sqlStmt instanceof MySqlShowKeysStatement) {
            rewriter = new DalMySqlShowKeysStatement();
        } else if (sqlStmt instanceof MySqlShowOpenTablesStatement) {
            rewriter = new DalMySqlShowOpenTablesStatement();
        } else if (sqlStmt instanceof MySqlShowTriggersStatement) {
            rewriter = new DalMySqlShowTriggersStatement();
        }
        if (rewriter != null) {
            rewriter.setSqlStmt(sqlStmt);
        }
        return rewriter;
    }
}
