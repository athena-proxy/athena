package me.ele.jarch.athena.sharding.sql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLInListExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;

import java.util.ArrayList;
import java.util.List;

// this class is for rewrite the table in where condition
// e.g. WHERE eleme_order.id = 1 -> WHERE eleme_order1.id = 1
public class OwnerWhereExprHandler extends WhereExprHandler {
    private String originTable;
    private final char literalSymbol;

    private List<SQLPropertyExpr> whereRewriteOwnersList = new ArrayList<>();

    public OwnerWhereExprHandler(String originTable, final char literalSymbol) {
        this.originTable = originTable;
        this.literalSymbol = literalSymbol;
    }

    public void rewriteWhereOwners(String shardedTable) {
        whereRewriteOwnersList.forEach(p -> p.setOwner(new SQLIdentifierExpr(shardedTable)));
    }

    private void addWhereRewriteOwner(SQLExpr e) {
        if (e == null) {
            return;
        }
        ShardingUtil
            .checkLiteralSymbolAndReplaceTable(e, originTable, (p) -> whereRewriteOwnersList.add(p),
                literalSymbol);
    }

    @Override protected void handleExpr(SQLExpr e) {
        addWhereRewriteOwner(e);
    }

    @Override protected void hanldeEquality(SQLBinaryOpExpr e) {
    }

    @Override protected void handleIN(SQLInListExpr inExpr) {
        addWhereRewriteOwner(inExpr.getExpr());
    }
};
