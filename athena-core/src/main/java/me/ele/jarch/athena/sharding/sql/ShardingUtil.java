package me.ele.jarch.athena.sharding.sql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.dialect.postgresql.ast.PGSQLObject;
import com.alibaba.druid.sql.visitor.SQLASTOutputVisitor;
import com.alibaba.druid.util.JdbcConstants;
import com.github.mpjct.jmpjct.util.ErrorCode;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.exception.QueryException;
import me.ele.jarch.athena.sharding.sql.dialect.mysql.visitor.DalMySqlOutputVisitor;
import me.ele.jarch.athena.sharding.sql.dialect.postgresql.visitor.DalPGOutputVisitor;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class ShardingUtil {
    public static String debugSql(SQLObject o, boolean printType) {
        StringBuilder sb = new StringBuilder(Constants.TYPICAL_SQL_SIZE);
        _debugSql(sb, o, printType);
        return sb.toString();
    }

    private static void _debugSql(StringBuilder sb, SQLObject o, boolean printType) {
        SQLASTOutputVisitor v =
            isPGObject(o) ? new DalPGOutputVisitor(sb) : new DalMySqlOutputVisitor(sb);
        o.accept(v);
        if (printType) {
            sb.append(" ").append(o.getClass().getSimpleName());
        }
    }

    public static String debugSql(SQLObject o) {
        return debugSql(o, false);
    }

    private static void _mostSafeSQLAndWhiteFields(SQLObject o, Appendable appender,
        Appendable whiteFieldsAppender, Predicate<String> whiteFieldsFilter) {
        SafeOutputVisitor v = isPGObject(o) ?
            new SafePGOutputVisitor(appender, whiteFieldsAppender, whiteFieldsFilter) :
            new SafeMySQLOutputVisitor(appender, whiteFieldsAppender, whiteFieldsFilter);
        v.getWhiteFieldsOutputProxy().setIfInsertStatement(o);
        o.accept(v);
    }

    public static SafeSQL mostSafeSQLAndWhiteFields(List<SQLStatement> sqlStatements,
        Predicate<String> whiteFieldsFilter) {
        return sqlStatements.size() == 1 ?
            singleMostSafeSQLAndWhiteFields(sqlStatements.get(0), whiteFieldsFilter) :
            multiMostSafeSQLAndWhiteFields(sqlStatements, whiteFieldsFilter);
    }

    private static SafeSQL singleMostSafeSQLAndWhiteFields(SQLStatement sqlStatement,
        Predicate<String> whiteFieldsFilter) {
        StringBuilder mostSafeSqlAppender = new StringBuilder(Constants.TYPICAL_SQL_SIZE);
        StringBuilder whiteFieldsAppender = new StringBuilder();
        _mostSafeSQLAndWhiteFields(sqlStatement, mostSafeSqlAppender, whiteFieldsAppender,
            whiteFieldsFilter);
        removeIfTailHasString(whiteFieldsAppender, "#");
        return new SafeSQL(mostSafeSqlAppender.toString(), whiteFieldsAppender.toString());
    }

    private static SafeSQL multiMostSafeSQLAndWhiteFields(List<SQLStatement> sqlStatements,
        Predicate<String> whiteFieldsFilter) {
        StringBuilder mostSafeSqlAppender = new StringBuilder(Constants.TYPICAL_SQL_SIZE);
        //白名单字段一般远小于SQL字段,所以无需预先指定初始大小
        StringBuilder whiteFieldsAppender = new StringBuilder();
        Set<String> patternFilter = new LinkedHashSet<>();
        final int len = sqlStatements.size();
        for (int i = 0; i < len; i++) {
            int previousWhiteFieldsLength = whiteFieldsAppender.length();
            StringBuilder tempMostSafeSqlAppender = new StringBuilder(Constants.TYPICAL_SQL_SIZE);
            _mostSafeSQLAndWhiteFields(sqlStatements.get(i), tempMostSafeSqlAppender,
                whiteFieldsAppender, whiteFieldsFilter);
            patternFilter.add(tempMostSafeSqlAppender.toString());
            removeIfTailHasString(whiteFieldsAppender, "#");
            if (i == len - 1) {
                break;
            }
            if (whiteFieldsAppender.length() - previousWhiteFieldsLength > 0) {
                whiteFieldsAppender.append(";");
            }
        }
        //防御空sql
        if (sqlStatements.size() == 0) {
            return new SafeSQL(mostSafeSqlAppender.toString(), whiteFieldsAppender.toString());
        }
        patternFilter.forEach(pattern -> mostSafeSqlAppender.append(pattern));
        removeIfTailHasString(mostSafeSqlAppender, ";");
        if (patternFilter.size() < sqlStatements.size()) {
            mostSafeSqlAppender.append("_x_N");
        }
        removeIfTailHasString(whiteFieldsAppender, ";");
        return new SafeSQL(mostSafeSqlAppender.toString(), whiteFieldsAppender.toString());
    }

    public static class SafeSQL {
        public final String mostSafeSql;
        public final String whiteFields;

        public SafeSQL(String mostSafeSql, String whiteFields) {
            this.mostSafeSql = mostSafeSql;
            this.whiteFields = whiteFields;
        }
    }

    public static String getVal(SQLExpr e) {
        if (e instanceof SQLBinaryOpExpr) {
            e = ((SQLBinaryOpExpr) e).getRight();
        }
        if (e instanceof SQLCharExpr) {
            StringBuilder sb = new StringBuilder();
            ((SQLCharExpr) e).output(sb);
            if (sb.charAt(0) == '\'' && sb.charAt(sb.length() - 1) == '\'') {
                sb.deleteCharAt(0).deleteCharAt(sb.length() - 1);
            }
            return sb.toString();
        } else if (e instanceof SQLIntegerExpr) {
            return ((SQLIntegerExpr) e).getValue().toString();
        } else if (e instanceof SQLNumberExpr) {
            return ((SQLNumberExpr) e).getNumber().toString();
        } else {
            throw new QueryException.Builder(ErrorCode.ERR_SQL_SYNTAX_ERROR)
                .setErrorMessage("unknown type, " + debugSql(e, true)).bulid();
        }
    }

    public static Number getNumberVal(SQLExpr e) {
        if (e instanceof SQLNumericLiteralExpr) {
            return ((SQLNumericLiteralExpr) e).getNumber();
        } else {
            throw new QueryException.Builder(ErrorCode.ERR_SQL_SYNTAX_ERROR)
                .setErrorMessage("it's not Number, " + debugSql(e, true)).bulid();
        }
    }

    public static String removeLiteralSymbol(String input, char literalSymbol) {
        if (input == null) {
            return input;
        }
        if (input.length() < 2) {
            return input;
        }
        if (input.charAt(0) == literalSymbol && input.charAt(input.length() - 1) == literalSymbol) {
            return input.substring(1, input.length() - 1);
        }
        return input;
    }

    // 检查传入的SQLExpr是否为SQLPropertyExpr,如果是,且owner为sharding的表名而非alias别名
    // 那么调用ReplaceTableMethod,进行一些操作,如替换,或者添加到某个集合等
    public static void checkLiteralSymbolAndReplaceTable(SQLExpr e, String tableNameLowerCase,
        Consumer<SQLPropertyExpr> method, char literalSymbol) {
        if (tableNameLowerCase == null) {
            return;
        }
        if (!(e instanceof SQLPropertyExpr)) {
            return;
        }
        SQLPropertyExpr p = (SQLPropertyExpr) e;
        if (!(p.getOwner() instanceof SQLIdentifierExpr)) {
            return;
        }
        String ownerName = ((SQLIdentifierExpr) p.getOwner()).getLowerName();
        ownerName = removeLiteralSymbol(ownerName, literalSymbol);
        if (tableNameLowerCase.equalsIgnoreCase(ownerName)) {
            method.accept(p);
        }
    }

    static void removeIfTailHasString(StringBuilder original, String targetStr) {
        int whiteFieldsLength = original.length();
        if (whiteFieldsLength > 0 && original.lastIndexOf(targetStr) == whiteFieldsLength - 1) {
            original.deleteCharAt(whiteFieldsLength - 1);
        }
    }

    public static boolean isPGObject(SQLObject o) {
        // 首先判定是否实现了PGSQLObject接口
        if (o instanceof PGSQLObject) {
            return true;
        }
        // 如果是通用语法结构,尝试通过SQLStatement的DbType判定是否是PG语法对象
        if (o instanceof SQLStatement) {
            return Objects.equals(JdbcConstants.POSTGRESQL, ((SQLStatement) o).getDbType());
        }
        // 如果前2种识别方式都不认为是PG语法对象,则不认为其为PG语法对象
        return false;
    }

}
