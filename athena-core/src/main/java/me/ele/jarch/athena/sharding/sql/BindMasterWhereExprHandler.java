package me.ele.jarch.athena.sharding.sql;

import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLInListExpr;

import static me.ele.jarch.athena.constant.Constants.BIND_MASTER;

public class BindMasterWhereExprHandler extends WhereExprHandler {

    private final ShardingSQL shardSql;

    public BindMasterWhereExprHandler(ShardingSQL shardSql) {
        super();
        this.shardSql = shardSql;
    }

    @Override protected void handleIN(SQLInListExpr inExpr) {
    }

    @Override
    // 业务需求, 如果SQL里包含where 'bind_master' = 'bind_master',那么直接走master库
    protected void hanldeEquality(SQLBinaryOpExpr e) {
        if (e == null) {
            return;
        }
        if (e.getOperator() != SQLBinaryOperator.Equality) {
            return;
        }
        if (!(e.getLeft() instanceof SQLCharExpr) || !(e.getRight() instanceof SQLCharExpr)) {
            return;
        }
        String left = ShardingUtil.getVal(e.getLeft());
        if (!BIND_MASTER.equals(left)) {
            return;
        }
        String right = ShardingUtil.getVal(e.getRight());
        if (!BIND_MASTER.equals(right)) {
            return;
        }
        shardSql.isContainsBindMasterExpr = true;
    }
}
