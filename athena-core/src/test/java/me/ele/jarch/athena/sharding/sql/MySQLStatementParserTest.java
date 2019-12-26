package me.ele.jarch.athena.sharding.sql;

import me.ele.jarch.athena.sharding.ShardingRouter;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * 为我们已知的明确要支持的奇怪语法添加单元测试以保证这些约束在druid版本升级中不被破坏掉
 * Created by jinghao.wang on 2017/12/18.
 */
public class MySQLStatementParserTest {
    private static void testParseSQL(String origin, String expect) {
        ShardingSQL shardingSQL = ShardingSQL
            .handleMySQLBasicly(origin, "", ShardingRouter.defaultShardingRouter,
                new Send2BatchCond(), ShardingSQL.BLACK_FIELDS_FILTER);
        String actual = ShardingUtil.debugSql(shardingSQL.sqlStmt);
        Assert.assertEquals(actual, expect);
    }

    @Test public void testArithmeticSymbol() throws Exception {
        String sql =
            "update tbl_sub_account set amount = amount-9 where sub_account_id = 201710250000148195 and customer_id = 77";
        String expect =
            "UPDATE tbl_sub_account SET amount = amount - 9 WHERE sub_account_id = 201710250000148195 AND customer_id = 77";
        testParseSQL(sql, expect);
    }

    @Test public void testArithmeticSymbol1() throws Exception {
        String sql =
            "update tbl_sub_account set amount = amount-10 where sub_account_id = 201710250000148195 and customer_id = 77";
        String expect =
            "UPDATE tbl_sub_account SET amount = amount - 10 WHERE sub_account_id = 201710250000148195 AND customer_id = 77";
        testParseSQL(sql, expect);
    }

    @Test public void testArithmeticSymbol2() throws Exception {
        String sql =
            "update tbl_sub_account set amount=amount-10 where sub_account_id = 201710250000148195 and customer_id = 77";
        String expect =
            "UPDATE tbl_sub_account SET amount = amount - 10 WHERE sub_account_id = 201710250000148195 AND customer_id = 77";
        testParseSQL(sql, expect);
    }

    @Test public void testArithmeticSymbol3() throws Exception {
        String sql =
            "update tbl_sub_account set amount=amount-9 where sub_account_id = 201710250000148195 and customer_id = 77";
        String expect =
            "UPDATE tbl_sub_account SET amount = amount - 9 WHERE sub_account_id = 201710250000148195 AND customer_id = 77";
        testParseSQL(sql, expect);
    }

    @Test public void testSelectFoundRows() throws Exception {
        String sql = "select FOUND_ROWS()";
        String expect = "SELECT FOUND_ROWS()";
        testParseSQL(sql, expect);
    }

    @Test public void testSelectFoundRows1() throws Exception {
        String sql = "select found_rows()";
        String expect = "SELECT found_rows()";
        testParseSQL(sql, expect);
    }

    @Test public void testSelectCalcFoundRows() throws Exception {
        String sql = "select SQL_CALC_FOUND_ROWS name from atest limit 2";
        String expect = "SELECT SQL_CALC_FOUND_ROWS name FROM atest LIMIT 2";
        testParseSQL(sql, expect);
    }

    @Test public void testNoWait() throws Exception {
        String sql = "select name from atest where id = 69 for update nowait";
        String expect = "SELECT name FROM atest WHERE id = 69 FOR UPDATE NOWAIT";
        testParseSQL(sql, expect);
    }

    @Test public void testUnsignedBigInt() throws Exception {
        String sql = "SELECT a from b where c <> 1 LIMIT 18446744073709551615 OFFSET 1";
        String expected = "SELECT a FROM b WHERE c <> 1 LIMIT 1, 18446744073709551615";
        testParseSQL(sql, expected);
    }
}
