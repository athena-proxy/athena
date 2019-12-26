package me.ele.jarch.athena.sharding.sql;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import com.alibaba.druid.sql.parser.SQLParserUtils;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.util.JdbcUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class MysqlParameterizedOutputVisitorTest {
    String selectSql =
        "SELECT eleme_order.id FROM eleme_order WHERE eleme_order.created_at >= 1 AND eleme_order.user_id = 100 "
            + "AND eleme_order.restaurant_id IN (200,300) AND distance < 3.0 ORDER BY eleme_order.created_at DESC LIMIT 100";
    String insertSql =
        "insert into eleme_order(id, user_id, restaurant_id, task_hash, date, index) values(1, 2, 3, '21329173', '2016-07-01', 999),(2, 3, 4, '21329173', '2016-07-01', 999)";
    String insertSelectSql =
        "insert into eleme_order(id, user_id, restaurant_id, task_hash, date, index) select id, user_id, restaurant_id, task_hash, date, index from eleme_order1_1";
    String updateSql =
        "update eleme_order Set price = 9.9 where eleme_order.id = 1 AND pre_price = 12.1 AND user_name is not null And rate='b' aNd field1 = 1.2";

    SQLStatement selectStmt = null;
    SQLStatement insertStmt = null;
    SQLStatement insertSelectStmt = null;
    SQLStatement updateStmt = null;

    @BeforeClass public void init() {
        SQLStatementParser parser =
            SQLParserUtils.createSQLStatementParser(selectSql, JdbcUtils.MYSQL);
        selectStmt = parser.parseStatement();

        SQLStatementParser parser1 =
            SQLParserUtils.createSQLStatementParser(insertSql, JdbcUtils.MYSQL);
        insertStmt = parser1.parseStatement();

        SQLStatementParser parser2 =
            SQLParserUtils.createSQLStatementParser(insertSelectSql, JdbcUtils.MYSQL);
        insertSelectStmt = parser2.parseStatement();

        SQLStatementParser parser3 =
            SQLParserUtils.createSQLStatementParser(updateSql, JdbcUtils.MYSQL);
        updateStmt = parser3.parseStatement();
    }

    @Test public void testSetTable() throws Exception {
        StringBuilder sb = new StringBuilder();
        MySqlOutputVisitor v;
        v = new MySqlOutputVisitor(sb, true);
        v.setShardingSupport(false);
        v.setPrettyFormat(false);
        selectStmt.accept(v);
        String expected =
            "SELECT eleme_order.id FROM eleme_order WHERE eleme_order.created_at >= ? AND eleme_order.user_id = ? AND eleme_order.restaurant_id IN (?) AND distance < ? ORDER BY eleme_order.created_at DESC LIMIT ?";
        Assert.assertEquals(sb.toString(), expected);
    }

    @Test public void testInsertSql() throws Exception {
        StringBuilder sb = new StringBuilder();
        MySqlOutputVisitor v;
        v = new MySqlOutputVisitor(sb, true);
        v.setShardingSupport(false);
        v.setPrettyFormat(false);
        insertStmt.accept(v);
        String expected =
            "INSERT INTO eleme_order (id, user_id, restaurant_id, task_hash, date , index) VALUES (?, ?, ?, ?, ? , ?)";
        Assert.assertEquals(sb.toString(), expected);
    }

    @Test public void testSetNonInsertStmt() {
        StringBuilder sb = new StringBuilder();
        MySqlOutputVisitor v;
        v = new MySqlOutputVisitor(sb, true);
        v.setShardingSupport(false);
        v.setPrettyFormat(false);
        insertStmt.accept(v);
        String expected =
            "INSERT INTO eleme_order (id, user_id, restaurant_id, task_hash, date , index) VALUES (?, ?, ?, ?, ? , ?)";
        Assert.assertEquals(sb.toString(), expected);
    }

    @Test public void testInsertSelectSql() throws Exception {
        StringBuilder sb = new StringBuilder();
        MySqlOutputVisitor v;
        v = new MySqlOutputVisitor(sb, true);
        v.setShardingSupport(false);
        v.setPrettyFormat(false);
        insertSelectStmt.accept(v);
        String expected =
            "INSERT INTO eleme_order (id, user_id, restaurant_id, task_hash, date , index) SELECT id, user_id, restaurant_id, task_hash, date , index FROM eleme_order1_1";
        Assert.assertEquals(sb.toString(), expected);
    }

    @Test public void testUpdateSql() {
        StringBuilder sb = new StringBuilder();
        MySqlOutputVisitor v;
        v = new MySqlOutputVisitor(sb, true);
        v.setShardingSupport(false);
        v.setPrettyFormat(false);
        updateStmt.accept(v);
        String expected =
            "UPDATE eleme_order SET price = ? WHERE eleme_order.id = ? AND pre_price = ? AND user_name IS NOT NULL AND rate = ? AND field1 = ?";
        Assert.assertEquals(sb.toString(), expected);
    }
}
