package me.ele.jarch.athena.sharding.sql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLLimit;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLInListExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement.ValuesClause;
import com.alibaba.druid.sql.visitor.SQLASTOutputVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.function.Predicate;

/**
 * Created by Dal-Dev-Team on 16/12/12.
 */
public class WhiteFieldsOutputProxy {
    private static final Logger LOGGER = LoggerFactory.getLogger(WhiteFieldsOutputProxy.class);

    private static final char WHITE_FIELD_SPLITER = '#';

    private final Appendable whiteFieldsAppender;

    private final SQLASTOutputVisitor whiteFieldsVistor;

    private SQLInsertStatement insert = null;

    private final Predicate<String> whiteFieldsFilter;

    public WhiteFieldsOutputProxy(SQLASTOutputVisitor whiteFieldsVistor,
        Predicate<String> whiteFieldsFilter) {
        this.whiteFieldsVistor = whiteFieldsVistor;
        this.whiteFieldsAppender = whiteFieldsVistor.getAppender();
        this.whiteFieldsFilter = whiteFieldsFilter;

    }

    // 因为insert无法从values语句中获取值所对应的列名,所以需要SQLInsertStatement对象以获取更多信息
    public void setIfInsertStatement(SQLObject statement) {
        if (!(statement instanceof SQLInsertStatement)) {
            return;
        }
        this.insert = (SQLInsertStatement) statement;
    }

    // 判断该表达式是否需要打印原始信息
    // 如eleme_order.user_id = 100,该函数的参数即eleme_order.user_id所代表的SQLPropertyExpr对象
    protected boolean isTheLeftExprNeedPrintOrigin(SQLExpr expr) {
        if (expr instanceof SQLIdentifierExpr) {
            SQLIdentifierExpr e = (SQLIdentifierExpr) expr;
            if (whiteFieldsFilter.test(e.getName())) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(String.format("reserve info of column: %s in log", e.getName()));
                }
                return true;
            }
        }
        if (expr instanceof SQLPropertyExpr) {
            SQLPropertyExpr e = (SQLPropertyExpr) expr;
            if (whiteFieldsFilter.test(e.getName())) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(String.format("reserve info of column: %s in log", e.getName()));
                }
                return true;
            }
        }
        return false;
    }

    // 判断该值是否需要打印原始信息
    // 如eleme_order.user_id = 100,该函数的参数即100所代表的SQLIntegerExpr对象
    protected boolean printOriginIfTheRightExprNeed(SQLExpr x) throws IOException {
        if (x instanceof SQLInListExpr) {
            SQLInListExpr in = (SQLInListExpr) x;
            if (isTheLeftExprNeedPrintOrigin(in.getExpr())) {
                if (!in.getTargetList().isEmpty()) {
                    in.accept(whiteFieldsVistor);
                    whiteFieldsAppender.append(WHITE_FIELD_SPLITER);
                }
                return true;
            }
        }

        if (x.getParent() == null) {
            return false;
        }
        if (x.getParent() instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr binary = (SQLBinaryOpExpr) x.getParent();
            if (isTheLeftExprNeedPrintOrigin(binary.getLeft())) {
                binary.accept(whiteFieldsVistor);
                whiteFieldsAppender.append(WHITE_FIELD_SPLITER);
                return true;
            }
        }

        return false;
    }

    public void printValue(SQLExpr x) {
        try {
            printOriginIfTheRightExprNeed(x);
        } catch (Exception e) {
            LOGGER.error("", e);
        }
    }

    public void printValue(ValuesClause valuesClause) {
        if (insert == null) {
            return;
        }
        try {
            List<SQLExpr> columns = insert.getColumns();
            List<SQLExpr> values = valuesClause.getValues();
            int size = columns.size() < values.size() ? columns.size() : values.size();
            for (int i = 0; i < size; i++) {
                SQLExpr column = columns.get(i);
                if (isTheLeftExprNeedPrintOrigin(column)) {
                    column.accept(whiteFieldsVistor);
                    whiteFieldsAppender.append("->");
                    values.get(i).accept(whiteFieldsVistor);
                    whiteFieldsAppender.append(WHITE_FIELD_SPLITER);
                }
            }
        } catch (Exception e) {
            LOGGER.error("", e);
        }
    }

    public boolean printValue(SQLLimit x) {
        try {
            whiteFieldsAppender.append("LIMIT ");
            x.getRowCount().accept(whiteFieldsVistor);
            if (x.getOffset() != null) {
                whiteFieldsAppender.append(" OFFSET ");
                x.getOffset().accept(whiteFieldsVistor);
            }
            whiteFieldsAppender.append(WHITE_FIELD_SPLITER);
        } catch (IOException e) {
            LOGGER.error("Limit:", e);
        }
        return false;
    }

}
