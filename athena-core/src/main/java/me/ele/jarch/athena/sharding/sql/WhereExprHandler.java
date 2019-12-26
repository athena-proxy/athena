package me.ele.jarch.athena.sharding.sql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBetweenExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLInListExpr;

import java.util.ArrayDeque;
import java.util.Deque;

public abstract class WhereExprHandler {
    // WHERE id IN (101, 103)
    protected abstract void handleIN(SQLInListExpr inExpr);

    // WHERE id = 102
    // WHERE id = '102'
    protected abstract void hanldeEquality(SQLBinaryOpExpr e);

    protected void handleExpr(SQLExpr e) {
    }

    protected void checkAndOrLogic() {
    }

    final public void parseWhere(SQLExpr whereExpr) {
        if (whereExpr == null) {
            return;
        }
        Deque<SQLExpr> deque = new ArrayDeque<>();
        deque.add(whereExpr);
        while (!deque.isEmpty()) {
            SQLExpr sqlExpr = deque.poll();
            handleExpr(sqlExpr);
            if (sqlExpr instanceof SQLInListExpr) {
                handleIN((SQLInListExpr) sqlExpr);
                continue;
            }
            if (sqlExpr instanceof SQLBetweenExpr) {
                SQLBetweenExpr between = (SQLBetweenExpr) sqlExpr;
                deque.add(between.getTestExpr());
                continue;
            }
            if (sqlExpr instanceof SQLBinaryOpExpr) {
                SQLBinaryOpExpr e = (SQLBinaryOpExpr) sqlExpr;
                SQLBinaryOperator op = e.getOperator();
                if (op == SQLBinaryOperator.Equality) {
                    hanldeEquality(e);
                }
                deque.add(e.getLeft());
                deque.add(e.getRight());
                continue;
            }
        }
        checkAndOrLogic();
    }
}
