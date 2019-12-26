package me.ele.jarch.athena.sharding.sql.dialect.postgresql.visitor;

import com.alibaba.druid.sql.ast.expr.SQLInListExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.statement.SQLReleaseSavePointStatement;
import com.alibaba.druid.sql.ast.statement.SQLRollbackStatement;
import com.alibaba.druid.sql.ast.statement.SQLSavePointStatement;
import com.alibaba.druid.sql.ast.statement.SQLStartTransactionStatement;
import com.alibaba.druid.sql.dialect.postgresql.visitor.PGOutputVisitor;
import com.alibaba.druid.sql.visitor.ExportParameterVisitorUtils;
import com.alibaba.druid.sql.visitor.VisitorFeature;
import me.ele.jarch.athena.sharding.sql.dialect.postgresql.statment.DalPGSQLReleaseSavePointStatement;

import java.math.BigInteger;
import java.util.Objects;

/**
 * Created by jinghao.wang on 2017/12/12.
 */
public class DalPGOutputVisitor extends PGOutputVisitor {
    public DalPGOutputVisitor(Appendable appender) {
        this(appender, false);
    }

    public DalPGOutputVisitor(Appendable appender, boolean parameterized) {
        super(appender, parameterized);
        config(VisitorFeature.OutputPrettyFormat, false);
        shardingSupport = false;
    }

    @Override public boolean visit(SQLSavePointStatement x) {
        if (!parameterized) {
            return super.visit(x);
        }
        print0(ucase ? "SAVEPOINT" : "savepoint");
        if (x.getName() != null) {
            print(" ?");
        }
        return false;
    }

    @Override public boolean visit(SQLReleaseSavePointStatement x) {
        if (!parameterized) {
            return super.visit(x);
        }
        print0(ucase ? "RELEASE " : "release  ");
        if (x instanceof DalPGSQLReleaseSavePointStatement) {
            DalPGSQLReleaseSavePointStatement dalReleaseStmt =
                (DalPGSQLReleaseSavePointStatement) x;
            if (Objects.nonNull(dalReleaseStmt.getSavepoint())) {
                print0(ucase ? "SAVEPOINT " : "savepoint  ");
            }
        }
        if (x.getName() != null) {
            print("?");
        }
        return false;
    }

    @Override public boolean visit(SQLRollbackStatement x) {
        if (!parameterized) {
            return super.visit(x);
        }
        print0(ucase ? "ROLLBACK" : "rollback");
        if (x.getTo() != null) {
            print0(ucase ? " TO " : " to ");
            print("?");
        }
        return false;
    }

    @Override public boolean visit(SQLStartTransactionStatement x) {
        if (x.isBegin()) {
            print0(ucase ? "BEGIN" : "begin");
            if (x.isWork()) {
                print0(ucase ? " WORK" : " work");
            }
            // TODO 对于PostgreSQL, BEGIN后还可能有TRANSACTION关键字,需要等待上游druid版本支持,目前只支持裸写BEGIN
            return false;
        }
        // 万一上游更改了SQLStartTransactionStatement含义,需要实现保证至少能够支持START TRANSACTION
        print0(ucase ? "START TRANSACTION" : "start transaction");
        return false;
    }

    @Override public void endVisit(SQLStartTransactionStatement x) {
        super.endVisit(x);
    }

    /**
     * druid对超过Long.MAX_VALUE的整数值输出会溢出，此方法覆盖父类有问题的版本,修复问题。
     * {@code Lexer::integerValue}  根据数字大小构造数字类型，只有超出 {@code Long} 范围的才使用 {@code BigInteger}
     *
     * @param x
     * @param parameterized
     */
    @Override protected void printInteger(SQLIntegerExpr x, boolean parameterized) {
        Number number = x.getNumber();
        if (!(number instanceof BigInteger)) {
            super.printInteger(x, parameterized);
            return;
        }


        String val = number.toString();
        if (parameterized) {
            print('?');
            incrementReplaceCunt();

            if (this.parameters != null) {
                ExportParameterVisitorUtils.exportParameter(this.parameters, x);
            }
            return;
        }

        print(val);
    }

    /**
     * 通过复写父类方法实现来修复1.1.6版本的druid对
     * `SELECT id FROM t_wms_stock WHERE (warehouse_id, material_id) IN ((1, 2), (3,4), (5,6),(7,8))`
     * 生成SQL pattern时处理为
     * `SELECT id FROM t_wms_stock WHERE (warehouse_id, material_id) IN ((?, ?),(?, ?),(?, ?),(?, ?))`
     * 对Athena的使用场景来说,处理成如下
     * `SELECT id FROM t_wms_stock WHERE (warehouse_id, material_id) IN (?)` 更加符合需求。
     * 此处修改为无论In表达式中是什么子表达式在生成pattern时都无脑变为 IN (?)
     *
     * @param x
     * @return
     */
    @Override public boolean visit(SQLInListExpr x) {
        if (!parameterized) {
            return super.visit(x);
        }
        printExpr(x.getExpr());
        if (x.isNot()) {
            print(ucase ? " NOT IN (?)" : " not in (?)");
        } else {
            print(ucase ? " IN (?)" : " in (?)");
        }
        return false;
    }
}
