package me.ele.jarch.athena.sharding.sql;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.postgresql.visitor.PGOutputVisitor;
import com.alibaba.druid.sql.parser.SQLParserUtils;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.util.JdbcUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;

public class SafePGSQLOutputVisitorTest {
    String selectSql =
        "SELECT eleme_order.id FROM eleme_order WHERE eleme_order.created_at >= 1 AND eleme_order.user_id = 100 "
            + "AND eleme_order.restaurant_id IN (200,300) AND distance < 3.0 ORDER BY eleme_order.created_at DESC LIMIT 100";
    String insertSql =
        "insert into eleme_order(id, user_id, restaurant_id, task_hash, date, index) values(1, 2, 3, '21329173', '2016-07-01', 999),(2, 3, 4, '213291452', '2016-07-01', 999)";
    String updateSql =
        "update eleme_order Set price = 9.9 where eleme_order.id = 1 AND pre_price = 12.1 AND user_name is not null And rate='b' aNd field1 = 1.2";

    SQLStatement selectStmt = null;
    SQLStatement insertStmt = null;
    SQLStatement updateStmt = null;
    Predicate<String> whiteFieldsFilter = null;

    @BeforeClass public void init() {
        Set<String> whiteFields = new TreeSet<>();
        whiteFields.add("id");
        whiteFields.add("user_id");
        whiteFields.add("restaurant_id");
        whiteFields.add("task_hash");
        whiteFields.add("field1");
        whiteFields.add("field2");
        whiteFields.add("field3");
        whiteFieldsFilter = whiteFields::contains;
        SQLStatementParser parser =
            SQLParserUtils.createSQLStatementParser(selectSql, JdbcUtils.POSTGRESQL);
        selectStmt = parser.parseStatement();

        SQLStatementParser parser1 =
            SQLParserUtils.createSQLStatementParser(insertSql, JdbcUtils.POSTGRESQL);
        insertStmt = parser1.parseStatement();

        SQLStatementParser parser2 =
            SQLParserUtils.createSQLStatementParser(updateSql, JdbcUtils.POSTGRESQL);
        updateStmt = parser2.parseStatement();
    }

    @Test public void testSetTable() throws Exception {
        StringBuilder mostSafeSb = new StringBuilder();
        StringBuilder whiteFieldsSb = new StringBuilder();
        SafePGOutputVisitor v;
        v = new SafePGOutputVisitor(mostSafeSb, whiteFieldsSb, whiteFieldsFilter);
        v.setPrettyFormat(false);
        selectStmt.accept(v);
        String expectedMostSafeSql =
            "SELECT eleme_order.id FROM eleme_order WHERE eleme_order.created_at >= ? AND eleme_order.user_id = ? AND eleme_order.restaurant_id IN (?) AND distance < ? ORDER BY eleme_order.created_at DESC LIMIT ?";
        String expectedWhiteFields =
            "eleme_order.user_id = 100#eleme_order.restaurant_id IN (200, 300)#";
        Assert.assertEquals(mostSafeSb.toString(), expectedMostSafeSql);
        Assert.assertEquals(whiteFieldsSb.toString(), expectedWhiteFields);
        Assert.assertTrue(mostSafeSb.toString().contains("user_id = ?"));
        Assert.assertTrue(mostSafeSb.toString().contains("restaurant_id IN (?)"));
        Assert.assertTrue(whiteFieldsSb.toString().contains("user_id = 100"));
        Assert.assertTrue(whiteFieldsSb.toString().contains("restaurant_id IN (200, 300)"));
    }

    @Test public void testInsertSql() throws Exception {
        StringBuilder mostSafeSb = new StringBuilder();
        StringBuilder whiteFieldsSb = new StringBuilder();
        SafePGOutputVisitor v;
        v = new SafePGOutputVisitor(mostSafeSb, whiteFieldsSb, whiteFieldsFilter);
        v.getWhiteFieldsOutputProxy().setIfInsertStatement(insertStmt);
        v.setPrettyFormat(false);
        insertStmt.accept(v);
        String desensitizedStr = whiteFieldsSb.toString();
        String expectedMostSafeSql =
            "INSERT INTO eleme_order (id, user_id, restaurant_id, task_hash, date , index) VALUES (?)";
        String expectedWhiteFields = "id->1#user_id->2#restaurant_id->3#task_hash->'21329173'#";
        Assert.assertEquals(mostSafeSb.toString(), expectedMostSafeSql);
        Assert.assertEquals(whiteFieldsSb.toString(), expectedWhiteFields);
        Assert.assertTrue(mostSafeSb.toString().contains("INSERT"));
        Assert.assertTrue(desensitizedStr.contains("id"));
        Assert.assertTrue(desensitizedStr.contains("user_id"));
        Assert.assertTrue(desensitizedStr.contains("restaurant_id"));
        Assert.assertTrue(desensitizedStr.contains("task_hash"));
        Assert.assertFalse(desensitizedStr.contains("date"));
        Assert.assertFalse(desensitizedStr.contains("index"));
    }

    @Test public void testSetNonInsertStmt() {
        StringBuilder mostSafeSb = new StringBuilder();
        StringBuilder whiteFieldsSb = new StringBuilder();
        SafePGOutputVisitor v =
            new SafePGOutputVisitor(mostSafeSb, whiteFieldsSb, whiteFieldsFilter);
        v.getWhiteFieldsOutputProxy().setIfInsertStatement(selectStmt);
        v.setPrettyFormat(false);
        insertStmt.accept(v);
        Assert.assertTrue(whiteFieldsSb.toString().isEmpty());
    }

    @Test public void testUpdateSql() {
        StringBuilder mostSafeSb = new StringBuilder();
        StringBuilder whiteFieldsSb = new StringBuilder();
        SafePGOutputVisitor v =
            new SafePGOutputVisitor(mostSafeSb, whiteFieldsSb, whiteFieldsFilter);
        v.setPrettyFormat(false);
        updateStmt.accept(v);
        Assert.assertTrue(mostSafeSb.toString().contains("UPDATE"));
        Assert.assertFalse(whiteFieldsSb.toString().isEmpty());
        Assert.assertTrue(whiteFieldsSb.toString().contains("1"));
        Assert.assertTrue(whiteFieldsSb.toString().contains("1.2"));
    }

    @Test(enabled = true) public void testOriginalPGOutPutVistor() throws Exception {
        StringBuilder debugSb = new StringBuilder();
        PGOutputVisitor v = new PGOutputVisitor(debugSb);
        v.setPrettyFormat(true);
        selectStmt.accept(v);
        Assert.assertTrue(debugSb.toString().contains("user_id = 100"));
        Assert.assertTrue(debugSb.toString().contains("restaurant_id IN (200, 300)"));
    }
}
