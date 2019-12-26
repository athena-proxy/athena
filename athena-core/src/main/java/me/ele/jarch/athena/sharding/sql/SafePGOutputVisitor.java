package me.ele.jarch.athena.sharding.sql;

import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement.ValuesClause;
import me.ele.jarch.athena.sharding.sql.dialect.postgresql.visitor.DalPGOutputVisitor;

import java.util.List;
import java.util.function.Predicate;

public class SafePGOutputVisitor extends DalPGOutputVisitor implements SafeOutputVisitor {

    private final WhiteFieldsOutputProxy outputProxy;

    public SafePGOutputVisitor(Appendable appender, Appendable whiteFieldsAppender,
        Predicate<String> whiteFieldsFilter) {
        super(appender, true);
        outputProxy = new WhiteFieldsOutputProxy(new DalPGOutputVisitor(whiteFieldsAppender),
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

    @Override protected void printlnAndAccept(List<? extends SQLObject> nodes, String seperator) {
        // 针对insert做特殊处理，只解析第一个ValuesClause
        if (!nodes.isEmpty() && nodes.get(0) instanceof ValuesClause) {
            outputProxy.printValue((ValuesClause) nodes.get(0));
            print0("(?)");
            return;
        }
        super.printlnAndAccept(nodes, seperator);
    }
}
