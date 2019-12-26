package me.ele.jarch.athena.sharding.sql;

import me.ele.jarch.athena.allinone.DBVendor;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.sharding.ShardingRouter;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

/**
 * Created by jinghao.wang on 2017/11/6.
 */
public class PGShardingSQLTest extends BaseShardingSQLTest {
    private static ShardingSQL handleSQL(String sql, ShardingRouter shardingRouter) {
        String queryComment = "";
        if (sql.toLowerCase().contains("insert") || sql.toLowerCase().contains("delete")) {
            queryComment = "/* E:" + Constants.ELE_META_BATCH_ANALYZED_MARKER + "=true:E */";
        }
        Send2BatchCond batchCond = new Send2BatchCond().setBatchSendComment(queryComment);
        ShardingSQL shardingSQL = ShardingSQL
            .handlePGBasicly(sql, queryComment, shardingRouter, batchCond, whiteFieldsFilter);
        ShardingSQL.handleSQLSharding(shardingSQL);
        return shardingSQL;
    }

    @Test public void testSelectTableHasLiteralSymbol() throws Exception {
        String orginSQL =
            "select id, \"tb_shipping_order\".\"retailer_id\" as re_id, \"tb_shipping_order\".\"tracking_id\" as t_id from \"tb_shipping_order\" where t_id = 123";
        String expectSQL =
            "SELECT id, tb_shipping_order_61.\"retailer_id\" AS re_id, tb_shipping_order_61.\"tracking_id\" AS t_id FROM tb_shipping_order_61 WHERE t_id = 123";
        ShardingSQL shardingSQL = handleSQL(orginSQL, mixShardingRouter);
        assertTrue(shardingSQL.results.hasNext());
        assertEquals(shardingSQL.results.next().shardedSQL, expectSQL);
        assertFalse(shardingSQL.results.hasNext());
    }

    @Test public void testInsertTableHasLiteralSymbol() throws Exception {
        String orginSQL =
            "insert into \"tb_shipping_order\"(tracking_id,name) values('123', 'abc')";
        String expectSQL =
            "INSERT INTO tb_shipping_order_61 (tracking_id, name) VALUES ('123', 'abc')";
        ShardingSQL shardingSQL = handleSQL(orginSQL, mixShardingRouter);
        assertTrue(shardingSQL.results.hasNext());
        assertTrue(shardingSQL.results.next().shardedSQL.contains(expectSQL));
        assertFalse(shardingSQL.results.hasNext());
    }

    @Test public void testUpdateTableHasLiteralSymbol() throws Exception {
        String orginSQL =
            "update \"tb_shipping_order\" set name = 'abc' where \"tb_shipping_order\".\"tracking_id\" = '123'";
        String expectSQL =
            "UPDATE tb_shipping_order_61 SET name = 'abc' WHERE tb_shipping_order_61.\"tracking_id\" = '123'";
        ShardingSQL shardingSQL = handleSQL(orginSQL, mixShardingRouter);
        assertTrue(shardingSQL.results.hasNext());
        assertEquals(shardingSQL.results.next().shardedSQL, expectSQL);
        assertFalse(shardingSQL.results.hasNext());
    }

    @Test public void testDeleteTableHasLiteralSymbol() throws Exception {
        String orginSQL =
            "delete from \"tb_shipping_order\" where \"tb_shipping_order\".\"tracking_id\" = '123'";
        String expectSQL =
            "DELETE FROM tb_shipping_order_61 WHERE tb_shipping_order_61.\"tracking_id\" = '123'";
        ShardingSQL shardingSQL = handleSQL(orginSQL, mixShardingRouter);
        assertTrue(shardingSQL.results.hasNext());
        assertTrue(shardingSQL.results.next().shardedSQL.contains(expectSQL));
        assertFalse(shardingSQL.results.hasNext());
        assertEquals(shardingSQL.sqlFeature.vendor, DBVendor.PG);
        assertFalse(shardingSQL.sqlFeature.isOneKey());
    }

    @Test public void testTupleInExpr() throws Exception {
        String sql =
            "SELECT id FROM t_wms_stock WHERE (warehouse_id, material_id) IN ((1, 2), (3,4), (5,6),(7,8))";
        ShardingSQL shardingSQL = handleSQL(sql, ShardingRouter.defaultShardingRouter);
        assertEquals(shardingSQL.getOriginMostSafeSQL(),
            "SELECT id FROM t_wms_stock WHERE (warehouse_id, material_id) IN (?)");
    }

    @Test public void testFunctionInExpr() throws Exception {
        String sql =
            "SELECT shop_id, check_time_id, user_id FROM t_assistant_work_feedback WHERE work_date IN (DATE('2017-04-19 21:40:21'), DATE('2017-04-19 21:40:21'), DATE('2018-01-10 21:40:21'),DATE('2018-01-09 21:40:21'), DATE('2017-12-20 21:40:21'), DATE('2017-12-31 21:40:21'))";
        ShardingSQL shardingSQL = handleSQL(sql, ShardingRouter.defaultShardingRouter);
        assertEquals(shardingSQL.getOriginMostSafeSQL(),
            "SELECT shop_id, check_time_id, user_id FROM t_assistant_work_feedback WHERE work_date IN (?)");
    }

    @Test(description = "对于PostgreSQL, 双引号括起来的变量应该是非法的,但是1.1.6版本druid没有报错,在生成SQL pattern时将器作为标识符输出了")
    public void doubleQuotedExprPattern() throws Exception {
        String sql =
            "SELECT id, phone, settled_at, created_at, updated_at FROM eleme_order WHERE created_at >= \"2018-01-04 14:17:07\" AND created_at <= \"2018-01-04 14:18:07\"";
        ShardingSQL shardingSQL = handleSQL(sql, ShardingRouter.defaultShardingRouter);
        assertEquals(shardingSQL.getOriginMostSafeSQL(),
            "SELECT id, phone, settled_at, created_at, updated_at FROM eleme_order WHERE created_at >= \"2018-01-04 14:17:07\" AND created_at <= \"2018-01-04 14:18:07\"");
    }
}
