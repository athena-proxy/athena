package me.ele.jarch.athena.sharding.sql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;

// this class is for normal where condition parse
// if find required ID, set needSharding = true and add the id
public class ConditionWhereExprHandler extends WhereExprHandler {
    private ShardingSQL shardSql;

    public int shardingIndex = -1;

    public ConditionWhereExprHandler(ShardingSQL shardSql) {
        this.shardSql = shardSql;
    }

    @Override protected void handleIN(SQLInListExpr inExpr) {
        SQLExpr e = inExpr.getExpr();
        // if ID NOT IN (...) , skip this condition
        if (shardSql.needSharding && inExpr.isNot()) {
            return;
        }
        shardSql.exprNeedRewrite(e, false);
        shardSql.exprNeedRewrite(e, true);
    }

    @Override protected void hanldeEquality(SQLBinaryOpExpr e) {
        getShardingIndex(e);
        shardSql.exprNeedRewrite(e.getLeft(), false);
        shardSql.exprNeedRewrite(e.getLeft(), true);
    }

    private static final String SHARDING_IDX = "sharding_index";

    private void getShardingIndex(SQLBinaryOpExpr e) {
        if (!(e.getLeft() instanceof SQLIdentifierExpr)) {
            return;
        }
        if (shardingIndex != -1) {
            return;
        }
        SQLIdentifierExpr ie = (SQLIdentifierExpr) e.getLeft();

        if (SHARDING_IDX.equals(ie.getName()) && e.getRight() instanceof SQLIntegerExpr) {
            shardingIndex = ((SQLIntegerExpr) e.getRight()).getNumber().intValue();
            e.setLeft(new SQLCharExpr(SHARDING_IDX));
            e.setRight(new SQLCharExpr(SHARDING_IDX));
        }
    }

    @Override protected void checkAndOrLogic() {
        // 过滤掉逻辑非法的sharding 条件
        shardSql.shardingCondition.composeColumnNameWithSQLExpr.forEach((col, expr) -> {
            if (isExprValid(expr)) {
                shardSql.validShardingCondtion.composeColumnNameWithSQLExpr.put(col, expr);
            }
        });
        shardSql.shardingCondition.shardingKeyNameWithSQLExpr.forEach((col, expr) -> {
            if (isExprValid(expr)) {
                shardSql.validShardingCondtion.shardingKeyNameWithSQLExpr.put(col, expr);
            }
        });
        shardSql.shardingCondition.mappingKeyNameWithSQLExpr.forEach((col, expr) -> {
            if (isExprValid(expr)) {
                shardSql.validShardingCondtion.mappingKeyNameWithSQLExpr.put(col, expr);
            }
        });
    }

    private boolean isExprValid(SQLExpr e) {
        while (e != null) {
            if (e instanceof SQLBinaryOpExpr) {
                if (((SQLBinaryOpExpr) e).getOperator() == SQLBinaryOperator.BooleanOr) {
                    return false;
                }
            }
            if (e.getParent() instanceof SQLExpr) {
                e = (SQLExpr) e.getParent();
            } else {
                return true;
            }
        }
        return true;
    }
}
