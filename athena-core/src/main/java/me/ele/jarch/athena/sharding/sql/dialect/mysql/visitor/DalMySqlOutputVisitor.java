package me.ele.jarch.athena.sharding.sql.dialect.mysql.visitor;

import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.SQLReleaseSavePointStatement;
import com.alibaba.druid.sql.ast.statement.SQLRollbackStatement;
import com.alibaba.druid.sql.ast.statement.SQLSavePointStatement;
import com.alibaba.druid.sql.ast.statement.SQLStartTransactionStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import com.alibaba.druid.sql.visitor.ExportParameterVisitorUtils;
import com.alibaba.druid.sql.visitor.VisitorFeature;
import me.ele.jarch.athena.constant.Constants;
import org.apache.commons.lang3.StringUtils;

import java.math.BigInteger;

/**
 * Created by jinghao.wang on 2017/12/12.
 */
public class DalMySqlOutputVisitor extends MySqlOutputVisitor {
    public DalMySqlOutputVisitor(Appendable appender) {
        this(appender, false);
    }

    public DalMySqlOutputVisitor(Appendable appender, boolean parameterized) {
        super(appender, parameterized);
        config(VisitorFeature.OutputPrettyFormat, false);
        shardingSupport = false;
    }

    /**
     * 对于'bind_master'='bind_master' 脱敏时保留值。
     *
     * @param x
     * @return
     */
    @Override public boolean visit(SQLCharExpr x) {
        if (!parameterized) {
            return super.visit(x);
        }

        if (!(x.getParent() instanceof SQLBinaryOpExpr)) {
            return super.visit(x);
        }

        SQLBinaryOpExpr parent = (SQLBinaryOpExpr) x.getParent();

        if (!parent.getOperator().equals(SQLBinaryOperator.Equality)) {
            return super.visit(x);
        }

        if (!(parent.getLeft() instanceof SQLCharExpr) || !(parent
            .getRight() instanceof SQLCharExpr)) {
            return super.visit(x);
        }

        SQLCharExpr left = (SQLCharExpr) parent.getLeft();
        SQLCharExpr right = (SQLCharExpr) parent.getRight();

        if (Constants.BIND_MASTER.equals(left.getText()) && Constants.BIND_MASTER
            .equals(right.getText())) {
            print0(ucase ? "BIND_MASTER" : "bind_master");
            return false;
        }
        return super.visit(x);
    }

    /**
     * TODO 1.1.6版本的druid对于MySQL `START TRANSACTION`和`BEGIN`语法的处理是完全错误的,此处复写父类实现以临时绕过问题,升级1.1.7版本后可以考虑删除此段代码
     *
     * @param x
     * @return
     */
    @Override public boolean visit(SQLStartTransactionStatement x) {
        if (x.isBegin()) {
            print0(ucase ? "BEGIN" : "begin");
            if (x.isWork()) {
                print0(ucase ? " WORK" : " work");
            }
            return false;
        }
        print0(ucase ? "START TRANSACTION" : "start transaction");
        if (x.isConsistentSnapshot()) {
            print0(ucase ? " WITH CONSISTENT SNAPSHOT" : " with consistent snapshot");
        }
        if (x.getHints() != null && x.getHints().size() > 0) {
            print(' ');
            printAndAccept(x.getHints(), " ");
        }
        return false;
    }

    @Override public void endVisit(SQLStartTransactionStatement x) {
        super.endVisit(x);
    }

    /**
     * druid对超过Long.MAX_VALUE的整数值输出会溢出，此方法覆盖父类有问题的版本,修复问题。
     * <p>
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

    /**
     * 通过复写父类方法实现来修复1.1.6版本的druid对
     * `SELECT id, phone, settled_at, created_at, updated_at FROM eleme_order WHERE created_at >= "2018-01-04 14:17:07" AND created_at <= "2018-01-04 14:18:07"`
     * 生成SQL pattern时处理为
     * `SELECT id, phone, settled_at, created_at, updated_at FROM eleme_order WHERE created_at >= "2018-01-04 14:17:07" AND created_at <= "2018-01-04 14:18:07"`
     * 对Athena的使用场景来来说,处理如下
     * `SELECT id, phone, settled_at, created_at, updated_at FROM eleme_order WHERE created_at >= ? AND created_at <= ?` 更加符合需求
     * druid在升级过程中产生了功能破坏，导致MySQL中双引号括起来的变量被识别为标识符,最终导致生成的SQL pattern无限增长。此处通过复写绕过此bug。
     * <p>
     * PostgreSQL不存在此问题,因为类似的SQL发到PostgreSQL,PostgreSQL会报语法错误,所以不会有业务这么写SQL。
     * 而MySQL原生认为单引号和双引号是等效的，所以类似的SQL发到MySQL能够正确执行。
     *
     * @param x
     * @return
     */
    @Override public boolean visit(SQLIdentifierExpr x) {
        if (!parameterized) {
            return super.visit(x);
        }
        if (!isDoubleQuotedName(x.getName())) {
            return super.visit(x);
        }
        print0("?");
        return false;
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

        if (x.getChain() != null) {
            if (x.getChain().booleanValue()) {
                print0(ucase ? " AND CHAIN" : " and chain");
            } else {
                print0(ucase ? " AND NO CHAIN" : " and no chain");
            }
        }

        if (x.getRelease() != null) {
            if (x.getRelease().booleanValue()) {
                print0(ucase ? " AND RELEASE" : " and release");
            } else {
                print0(ucase ? " AND NO RELEASE" : " and no release");
            }
        }

        if (x.getTo() != null) {
            print0(ucase ? " TO " : " to ");
            print("?");
        }

        return false;
    }


    private static boolean isDoubleQuotedName(final String name) {
        if (StringUtils.isEmpty(name)) {
            return false;
        }
        return name.startsWith("\"") && name.endsWith("\"");
    }
}
