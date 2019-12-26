package me.ele.jarch.athena.sharding.sql.dialect.postgresql.parser;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLStartTransactionStatement;
import com.alibaba.druid.sql.dialect.postgresql.parser.PGExprParser;
import com.alibaba.druid.sql.dialect.postgresql.parser.PGSQLStatementParser;
import com.alibaba.druid.sql.parser.Lexer;
import me.ele.jarch.athena.sharding.sql.dialect.postgresql.statment.DalPGSQLReleaseSavePointStatement;

import java.util.List;

/**
 * Created by jinghao.wang on 2017/12/12.
 */
public class DalPGSQLStatementParser extends PGSQLStatementParser {
    public DalPGSQLStatementParser(PGExprParser parser) {
        super(parser);
    }

    public DalPGSQLStatementParser(String sql) {
        this(new PGExprParser(sql));
    }

    public DalPGSQLStatementParser(Lexer lexer) {
        this(new PGExprParser(lexer));
    }

    @Override public SQLStatement parseReleaseSavePoint() {
        acceptIdentifier("RELEASE");
        DalPGSQLReleaseSavePointStatement stmt = new DalPGSQLReleaseSavePointStatement(getDbType());
        if (lexer.identifierEquals("SAVEPOINT")) {
            stmt.setSavepoint(this.exprParser.name());
        }
        stmt.setName(this.exprParser.name());
        return stmt;
    }

    @Override public boolean parseStatementListDialect(List<SQLStatement> statementList) {
        switch (lexer.token()) {
            case BEGIN:
                statementList.add(parseBegin());
                return true;
            default:
                return super.parseStatementListDialect(statementList);
        }
    }

    private SQLStatement parseBegin() {
        // PGStartTransactionStatement继承层级错误,此处直接使用SQLStartTransactionStatement类
        SQLStartTransactionStatement stmt = new SQLStartTransactionStatement();
        stmt.setDbType(this.dbType);
        stmt.setBegin(true);
        lexer.nextToken();
        return stmt;
    }
}
