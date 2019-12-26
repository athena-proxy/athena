package me.ele.jarch.athena.sharding.sql;

import com.alibaba.druid.sql.ast.SQLLimit;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import me.ele.jarch.athena.sharding.sql.dialect.mysql.visitor.DalMySqlOutputVisitor;

import java.util.List;
import java.util.function.Predicate;

public class SafeMySQLOutputVisitor extends DalMySqlOutputVisitor implements SafeOutputVisitor {

    private final WhiteFieldsOutputProxy outputProxy;

    public SafeMySQLOutputVisitor(Appendable appender, Appendable whiteFieldsAppender,
        Predicate<String> whiteFieldsFilter) {
        super(appender, true);
        outputProxy = new WhiteFieldsOutputProxy(new DalMySqlOutputVisitor(whiteFieldsAppender),
            whiteFieldsFilter);
    }

    @Override public WhiteFieldsOutputProxy getWhiteFieldsOutputProxy() {
        return outputProxy;
    }

    @Override public boolean visit(SQLCharExpr x) {
        outputProxy.printValue(x);
        return super.visit(x);
    }

    @Override public boolean visit(SQLIntegerExpr x) {
        outputProxy.printValue(x);
        return super.visit(x);
    }

    @Override public boolean visit(SQLNumberExpr x) {
        outputProxy.printValue(x);
        return super.visit(x);
    }

    @Override public boolean visit(SQLInListExpr x) {
        outputProxy.printValue(x);
        return super.visit(x);
    }

    @Override public boolean visit(SQLHexExpr x) {
        outputProxy.printValue(x);
        return super.visit(x);
    }

    @Override protected void printValuesList(List<SQLInsertStatement.ValuesClause> valuesList) {
        // 调用方已经判断过，如果valuesList为空或者null则不会进入该方法，所以下面不再做防御性判断。
        outputProxy.printValue(valuesList.get(0));
        print0(ucase ? "VALUES " : "values ");
        print0("(?)");
    }

    @Override public boolean visit(SQLLimit x) {
        outputProxy.printValue(x);
        return super.visit(x);
    }
}
