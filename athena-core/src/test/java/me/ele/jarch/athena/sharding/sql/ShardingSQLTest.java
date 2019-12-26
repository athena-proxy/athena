package me.ele.jarch.athena.sharding.sql;

import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLInListExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlUseIndexHint;
import com.github.mpjct.jmpjct.util.ErrorCode;
import me.ele.jarch.athena.allinone.AllInOneMonitor;
import me.ele.jarch.athena.allinone.DBVendor;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.exception.QueryException;
import me.ele.jarch.athena.scheduler.DBChannelDispatcher;
import me.ele.jarch.athena.sharding.ShardingConfig;
import me.ele.jarch.athena.sharding.ShardingRouter;
import me.ele.jarch.athena.sql.QUERY_TYPE;
import me.ele.jarch.athena.sql.seqs.GeneratorUtil;
import me.ele.jarch.athena.sql.seqs.SeqsException;
import me.ele.jarch.athena.sql.seqs.generator.ComposedGenerator;
import me.ele.jarch.athena.util.GreySwitch;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.testng.Assert.*;

public class ShardingSQLTest extends BaseShardingSQLTest {

    @FunctionalInterface private interface NormalFunc {
        void invoke();
    }


    @FunctionalInterface private interface ExceptionFunc {
        void invoke(QueryException e);
    }

    private void execute(NormalFunc func1, ExceptionFunc func2) {
        try {
            func1.invoke();
        } catch (QueryException e) { // NOSONAR
            func2.invoke(e);
        }
    }

    private ShardingSQL positiveTest(String originalSql, String changedSql1, String changedSql2,
        ShardingRouter shardingRouter) {
        return positiveTest(originalSql, changedSql1, changedSql2, shardingRouter, true);
    }

    private ShardingSQL positiveTest(String originalSql, String changedSql1, String changedSql2,
        ShardingRouter shardingRouter, boolean withBatchMarker) {
        expectResults.add(changedSql1);
        if (!changedSql2.isEmpty())
            expectResults.add(changedSql2);
        return positiveTest(originalSql, expectResults, shardingRouter, withBatchMarker);
    }

    private ShardingSQL positiveTest(String originalSql, List<String> expectResults,
        ShardingRouter shardingRouter) {
        return positiveTest(originalSql, expectResults, shardingRouter, true);
    }

    private ShardingSQL positiveTest(String originalSql, List<String> expectResults,
        ShardingRouter shardingRouter, boolean withBatchMarker) {
        ShardingSQL rewriter = handleSQL(originalSql, shardingRouter, withBatchMarker);
        while (rewriter.results.hasNext()) {
            String ssql = rewriter.results.next().shardedSQL;
            executeResults.add(ssql);
        }
        assertEquals(executeResults, expectResults);
        return rewriter;
    }

    private void negativeTest(String originalSql, ShardingRouter shardingRouter,
        ErrorCode errorCode) throws FileNotFoundException {
        this.execute(() -> {
            ShardingSQL rewriter = handleSQL(originalSql, shardingRouter);
            while (rewriter.results.hasNext()) {
                String ssql = rewriter.results.next().shardedSQL;
                executeResults.add(ssql);
            }
            fail();
        }, (e) -> {
            assertEquals(e.errorCode, errorCode);
        });
    }

    private void stateTest(String originalSql) throws FileNotFoundException {
        ShardingSQL rewriter = handleSQL(originalSql, mixShardingRouter);
        boolean state = rewriter.results.hasNext();
        assertEquals(state, false);
    }

    private void numberTest(String originalSql, int expectedNum) throws FileNotFoundException {
        int executeResultssum = 0;
        ShardingSQL rewriter = handleSQL(originalSql, mixShardingRouter);
        while (rewriter.results.hasNext()) {
            rewriter.results.next();
            executeResultssum++;
        }
        assertEquals(executeResultssum, expectedNum);
    }

    private void showTest(String originalSql, String changedSql, String dbName) {
        executeResults.clear();
        expectResults.clear();
        ShardingSQL rewriter = handleSQL(originalSql, mixShardingRouter);
        while (rewriter.results.hasNext()) {
            ShardingResult sr = rewriter.results.next();
            String ssql = sr.shardedSQL;
            String db = sr.shardDB;
            executeResults.add(ssql);
            assertEquals(db, dbName);
        }
        expectResults.add(changedSql);
        assertEquals(executeResults, expectResults);
    }

    private static ShardingSQL handleSQL(String sql, ShardingRouter shardingRouter) {
        return handleSQL(sql, shardingRouter, true);
    }

    private static ShardingSQL handleSQL(String sql, ShardingRouter shardingRouter,
        boolean withBatchMarker) {
        String queryComment = "";
        if ((sql.toLowerCase().contains("insert") || sql.toLowerCase().contains("delete"))
            && withBatchMarker) {
            queryComment = "/* E:" + Constants.ELE_META_BATCH_ANALYZED_MARKER + "=true:E */";
        }
        Send2BatchCond batchCond = new Send2BatchCond().setBatchSendComment(queryComment);
        ShardingSQL shardingSQL = ShardingSQL
            .handleMySQLBasicly(sql, queryComment, shardingRouter, batchCond, whiteFieldsFilter);
        ShardingSQL.handleSQLSharding(shardingSQL);
        return shardingSQL;
    }

    @Test public void testShardingSQL11() throws FileNotFoundException {
        String originalsql =
            "SELECT * FROM tb_shipping_order WHERE tb_shipping_order.tracking_id IN (10000, 20000, 30000, 40000)";
        String changedsql1 =
            "SELECT * FROM tb_shipping_order_152 WHERE tb_shipping_order_152.tracking_id IN (10000, 20000, 30000, 40000)";
        String changedsql2 =
            "SELECT * FROM tb_shipping_order_272 WHERE tb_shipping_order_272.tracking_id IN (10000, 20000, 30000, 40000)";
        String changedsql3 =
            "SELECT * FROM tb_shipping_order_32 WHERE tb_shipping_order_32.tracking_id IN (10000, 20000, 30000, 40000)";
        String changedsql4 =
            "SELECT * FROM tb_shipping_order_392 WHERE tb_shipping_order_392.tracking_id IN (10000, 20000, 30000, 40000)";
        ShardingSQL rewriter = handleSQL(originalsql, mixShardingRouter);
        while (rewriter.results.hasNext()) {
            String ssql = rewriter.results.next().shardedSQL;
            executeResults.add(ssql);
        }
        expectResults.add(changedsql1);
        expectResults.add(changedsql2);
        expectResults.add(changedsql3);
        expectResults.add(changedsql4);
        assertEquals(executeResults, expectResults);
    }

    @Test public void testShardingSQL12() throws FileNotFoundException {
        String originalsql = "SELECT * FROM tb_shipping_order where 1=2";
        numberTest(originalsql, 512);
    }

    @Test public void testShardingSQL13() throws FileNotFoundException {
        String originalsql = "set autocommit = 1";
        stateTest(originalsql);
    }

    @Test public void testShardingSQL14() throws FileNotFoundException {
        String originalsql = "commit";
        stateTest(originalsql);
    }

    @Test public void testShardingSQL15() throws FileNotFoundException {
        String originalsql = "rollback";
        stateTest(originalsql);
    }

    @Test public void testShardingSQL16() throws FileNotFoundException {
        String originalsql = "show collation where `Charset` = 'utf8' and `Collation` = 'utf8_bin'";
        stateTest(originalsql);
    }

    @Test public void testShardingSQL17() throws FileNotFoundException {
        String originalsql = "SHOW VARIABLES LIKE 'sql_mode'";
        stateTest(originalsql);
    }

    @Test public void testShardingSQL18() throws FileNotFoundException {
        String originalsql = "SET NAMES utf8";
        stateTest(originalsql);
    }

    @Test public void testShardingSQL19() throws FileNotFoundException {
        String originalsql = "SET abc=d";
        stateTest(originalsql);
    }

    @Test public void testShardingSQL20() throws FileNotFoundException {
        String originalsql = "select *from aeleme_order where id=25000";
        stateTest(originalsql);
    }

    @Test public void testShardingSQL27() throws FileNotFoundException {
        String originalsql = "drop table eleme_order";
        stateTest(originalsql);
    }

    @Test public void testShardingSQL32() throws FileNotFoundException {
        String originalsql = "select * from tb_shipping_order where retailer_id not in(10,110,510)";
        numberTest(originalsql, 512);
    }

    @Test public void testShardingSQL36() throws FileNotFoundException {
        String originalsql = "select count(*) from tb_shipping_order where tracking_id=10000";
        String changedsql = "SELECT COUNT(*) FROM tb_shipping_order_392 WHERE tracking_id = 10000";
        positiveTest(originalsql, changedsql, "", this.mixShardingRouter);
    }

    @Test public void testShardingSQL37() throws FileNotFoundException {
        String originalsql = "select sum(*) from tb_shipping_order where retailer_id=10000";
        String changedsql = "SELECT SUM(*) FROM tb_shipping_order_472 WHERE retailer_id = 10000";
        positiveTest(originalsql, changedsql, "", this.mixShardingRouter);
    }

    @Test public void testShardingSQLShardingIndex() throws FileNotFoundException {
        String originalsql =
            "SELECT xx, yy, zz FROM tb_shipping_order WHERE tb_shipping_order.status = 'xxxx' AND tb_shipping_order.updated_at BETWEEN xxxxxxx AND yyyyyyyyy AND sharding_index = 100 ORDER BY tb_shipping_order.updated_at LIMIT 200";
        String changedsql =
            "SELECT xx, yy, zz FROM tb_shipping_order_100 WHERE tb_shipping_order_100.status = 'xxxx' AND tb_shipping_order_100.updated_at BETWEEN xxxxxxx AND yyyyyyyyy AND 'sharding_index' = 'sharding_index' ORDER BY tb_shipping_order_100.updated_at LIMIT 200";
        positiveTest(originalsql, changedsql, "", this.mixShardingRouter);
    }

    @Test public void testShardingSQLShardingIndexUpdate() {
        String originalsql =
            "update tb_shipping_order force index(primary) set x = 3 where sharding_index = 100 and y = 4";
        String changedsql =
            "UPDATE tb_shipping_order_100 FORCE INDEX (PRIMARY) SET x = 3 WHERE 'sharding_index' = 'sharding_index' AND y = 4";
        ShardingSQL rewriter = handleSQL(originalsql, mixShardingRouter);
        while (rewriter.results.hasNext()) {
            String ssql = rewriter.results.next().shardedSQL;
            executeResults.add(ssql);
        }
        expectResults.add(changedsql);
        assertEquals(executeResults, expectResults);
    }

    @Test public void testShardingSQLShardingIndexDelete() {
        String originalsql =
            "update tb_shipping_order set x = 3 where sharding_index = 90 and y = 4";
        String changedsql =
            "UPDATE tb_shipping_order_90 SET x = 3 WHERE 'sharding_index' = 'sharding_index' AND y = 4";
        ShardingSQL rewriter = handleSQL(originalsql, mixShardingRouter);
        while (rewriter.results.hasNext()) {
            String ssql = rewriter.results.next().shardedSQL;
            executeResults.add(ssql);
        }
        expectResults.add(changedsql);
        assertEquals(executeResults, expectResults);
    }

    @Test(expectedExceptions = QueryException.class)
    public void testShardingSQLShardingIndexDelete2() {
        String originalsql = "delete FROM eleme_order WHERE sharding_index = 30";
        handleSQL(originalsql, newOrderShardingRouter);
    }

    @Test public void testShardingSQLBackQuota1() throws FileNotFoundException {
        String originalsql =
            "SELECT `tb_shipping_order`.`id` FROM `tb_shipping_order` WHERE `tb_shipping_order`.tracking_id = 123";
        String changedsql =
            "SELECT tb_shipping_order_61.`id` FROM tb_shipping_order_61 WHERE tb_shipping_order_61.tracking_id = 123";
        positiveTest(originalsql, changedsql, "", this.mixShardingRouter);
    }

    @Test public void testShardingSQLBackQuota2() throws FileNotFoundException {
        String originalsql =
            "SELECT tb_shipping_order.`id` FROM `tb_shipping_order` WHERE tb_shipping_order.tracking_id = 123";
        String changedsql =
            "SELECT tb_shipping_order_61.`id` FROM tb_shipping_order_61 WHERE tb_shipping_order_61.tracking_id = 123";
        positiveTest(originalsql, changedsql, "", this.mixShardingRouter);
    }

    @Test public void testShardingSQLBackQuota3() throws FileNotFoundException {
        String originalsql =
            "select `tb_shipping_order`.id from tb_shipping_order  where `tb_shipping_order`.`tracking_id` = 123";
        String changedsql =
            "SELECT tb_shipping_order_61.id FROM tb_shipping_order_61 WHERE tb_shipping_order_61.`tracking_id` = 123";
        positiveTest(originalsql, changedsql, "", this.mixShardingRouter);
    }

    @Test public void testShardingSQLBackQuota4() throws FileNotFoundException {
        String originalsql =
            "select `t1`.`id`,t1.user_name,`t1`.`description` from `tb_shipping_order` as `t1` where `t1`.`tracking_id`=123 and `t1`.`user_name`='w'";
        String changedsql =
            "SELECT `t1`.`id`, t1.user_name, `t1`.`description` FROM tb_shipping_order_61 t1 WHERE `t1`.`tracking_id` = 123 AND `t1`.`user_name` = 'w'";
        positiveTest(originalsql, changedsql, "", this.mixShardingRouter);
    }

    @Test public void testShardingSQLBackQuota5() throws FileNotFoundException {
        String originalsql =
            "insert into `tb_shipping_order`(`tracking_id`,`user_name`) values (1234567890,'test')";
        String changedsql = "/* E:" + Constants.ELE_META_BATCH_ANALYZED_MARKER
            + "=true:E */INSERT INTO tb_shipping_order_361 (`tracking_id`, `user_name`) VALUES (1234567890, 'test')";
        positiveTest(originalsql, changedsql, "", this.mixShardingRouter);
    }

    @Test public void testShardingSQLBackQuota6() throws FileNotFoundException {
        String originalsql =
            "update `tb_shipping_order` as `t1` set `t1`.`user_name`='newtest' where `t1`.`tracking_id`=1234567890";
        String changedsql =
            "UPDATE tb_shipping_order_361 t1 SET `t1`.`user_name` = 'newtest' WHERE `t1`.`tracking_id` = 1234567890";
        positiveTest(originalsql, changedsql, "", this.mixShardingRouter);
    }

    @Test public void testShardingSQLBackQuota7() throws FileNotFoundException {
        String originalsql =
            "delete from `tb_shipping_order` where `tb_shipping_order`.`tracking_id`=1234567890";
        String changedsql = "/* E:" + Constants.ELE_META_BATCH_ANALYZED_MARKER
            + "=true:E */DELETE FROM tb_shipping_order_361 WHERE tb_shipping_order_361.`tracking_id` = 1234567890";
        positiveTest(originalsql, changedsql, "", this.mixShardingRouter);
    }

    @Test public void testShardingSQLBackQuota8() throws FileNotFoundException {
        String originalsql =
            "delete from `tb_shipping_order` as t1 where `t1`.`tracking_id`=1234567890";
        String changedsql = "/* E:" + Constants.ELE_META_BATCH_ANALYZED_MARKER
            + "=true:E */DELETE FROM tb_shipping_order_361 t1 WHERE `t1`.`tracking_id` = 1234567890";
        positiveTest(originalsql, changedsql, "", this.mixShardingRouter);
    }

    @Test public void testShardingSQLBackQuota9() throws FileNotFoundException {
        String originalsql =
            "select `tb_shipping_order`.id from tb_shipping_order as tb_shipping_order where `tb_shipping_order`.`tracking_id` = 123";
        String changedsql =
            "SELECT `tb_shipping_order`.id FROM tb_shipping_order_61 tb_shipping_order WHERE `tb_shipping_order`.`tracking_id` = 123";
        positiveTest(originalsql, changedsql, "", this.mixShardingRouter);
    }

    @Test public void testShardingSQLBackQuota10() throws FileNotFoundException {
        String originalsql =
            "update `tb_shipping_order` as `tb_shipping_order` set `tb_shipping_order`.`user_name`='newtest' where `tb_shipping_order`.`tracking_id`=1234567890";
        String changedsql =
            "UPDATE tb_shipping_order_361 tb_shipping_order SET `tb_shipping_order`.`user_name` = 'newtest' WHERE `tb_shipping_order`.`tracking_id` = 1234567890";
        positiveTest(originalsql, changedsql, "", this.mixShardingRouter);
    }

    @Test public void testShardingSQLBackQuota11() throws FileNotFoundException {
        String originalsql =
            "select `tb_shipping_order`.id from tb_shipping_order as tb_shipping_order where `tb_shipping_order`.`tracking_id` = 123";
        String changedsql =
            "SELECT `tb_shipping_order`.id FROM tb_shipping_order_61 tb_shipping_order WHERE `tb_shipping_order`.`tracking_id` = 123";
        positiveTest(originalsql, changedsql, "", this.mixShardingRouter);
    }


    @Test public void testShardingSQL_groupby1() throws FileNotFoundException {
        String originalsql =
            "SELECT count(0) FROM tb_shipping_order group by tb_shipping_order.retailer_id";
        ShardingSQL rewriter = handleSQL(originalsql, mixShardingRouter);
        assertEquals(rewriter.results.groupByItems.get(0), "retailer_id");
        assertTrue(rewriter.sqlFeature.hasGroupBy());
        assertFalse(rewriter.sqlFeature.isOneKey());
    }

    @Test public void testShardingSQL39() throws FileNotFoundException {
        String originalsql =
            "SELECT tb_shipping_order.id AS id FROM tb_shipping_order WHERE tb_shipping_order.retailer_id NOT IN (2) AND tb_shipping_order.tracking_id IN (1)";
        String changedsql =
            "SELECT tb_shipping_order_0.id AS id FROM tb_shipping_order_0 WHERE tb_shipping_order_0.retailer_id NOT IN (2) AND tb_shipping_order_0.tracking_id IN (1)";
        positiveTest(originalsql, changedsql, "", this.mixShardingRouter);
    }

    @Test(description = "测试sharding条件之间使用OR逻辑条件连接在SELECT类型的情况下等效于没有带sharding条件,退化为扫全表")
    public void testShardingSQL40_non_validate_sharding_condition() throws FileNotFoundException {
        String originalsql =
            "SELECT tb_shipping_order.id AS id FROM tb_shipping_order WHERE tb_shipping_order.tracking_id IN (2) OR tb_shipping_order.retailer_id IN (1)";
        String changedSQLTemplate =
            "SELECT tb_shipping_order_%d.id AS id FROM tb_shipping_order_%d WHERE tb_shipping_order_%d.tracking_id IN (2) OR tb_shipping_order_%d.retailer_id IN (1)";
        List<String> expectedChangedSQLs = new ArrayList<>(512);
        IntStream.range(0, 512)
            .forEach(i -> expectedChangedSQLs.add(String.format(changedSQLTemplate, i, i, i, i)));
        positiveTest(originalsql, expectedChangedSQLs, this.mixShardingRouter);
    }

    /**
     * shoud use retailer_id sharding
     */
    @Test public void testShardingSQL_select_inner_or() throws FileNotFoundException {
        String originalsql =
            "SELECT * FROM tb_shipping_order WHERE tb_shipping_order.retailer_id = 100 and (tracking_id = 1500 or abc=123)";
        String changedsql =
            "SELECT * FROM tb_shipping_order_248 WHERE tb_shipping_order_248.retailer_id = 100 AND (tracking_id = 1500 OR abc = 123)";
        positiveTest(originalsql, changedsql, "", this.mixShardingRouter);
    }

    @Test public void testShardingSQLSelectForUpdate() throws FileNotFoundException {
        String originalsql =
            "select id from tb_shipping_order where id2=10000 and tracking_id=100 for update";
        String changedsql1 =
            "SELECT id FROM tb_shipping_order_50 WHERE id2 = 10000 AND tracking_id = 100 FOR UPDATE";
        ShardingSQL shardSql = positiveTest(originalsql, changedsql1, "", this.mixShardingRouter);
        String mostSafeSQL = shardSql.mostSafeSQL;
        String whiteFields = shardSql.whiteFields;
        assertEquals(mostSafeSQL,
            "SELECT id FROM tb_shipping_order WHERE id2 = ? AND tracking_id = ? FOR UPDATE");
        assertFalse(whiteFields.contains("id = 100"));
    }

    @Test public void testShardingSQLSelectForUpdateIn() throws FileNotFoundException {
        String originalsql =
            "select id from tb_shipping_order where id2=10000 and tracking_id in (100) for update";
        String changedsql1 =
            "SELECT id FROM tb_shipping_order_50 WHERE id2 = 10000 AND tracking_id IN (100) FOR UPDATE";
        positiveTest(originalsql, changedsql1, "", this.mixShardingRouter);
    }

    @Test(expectedExceptions = Exception.class) public void testShardingSQLSelectForUpdateNoWhere()
        throws FileNotFoundException {
        String originalsql = "select * from tb_shipping_order limit 1 for update";
        ShardingSQL rewriter = handleSQL(originalsql, mixShardingRouter);
        while (rewriter.results.hasNext()) {
            String ssql = rewriter.results.next().shardedSQL;
            executeResults.add(ssql);
        }
    }

    @Test(expectedExceptions = QueryException.class)
    public void testShardingSQLSelectForUpdateNoComposedId() throws FileNotFoundException {
        String originalsql = "select * from tb_shipping_order where id=111111 for update";
        ShardingSQL rewriter = handleSQL(originalsql, mixShardingRouter);
        while (rewriter.results.hasNext()) {
            String ssql = rewriter.results.next().shardedSQL;
            executeResults.add(ssql);
        }
    }

    @Test(expectedExceptions = QueryException.class)
    public void testShardingSQLSelectForUpdateMulComposedId() throws FileNotFoundException {
        String originalsql =
            "select * from tb_shipping_order where tracking_id in (1,2) for update";
        ShardingSQL rewriter = handleSQL(originalsql, mixShardingRouter);
        while (rewriter.results.hasNext()) {
            String ssql = rewriter.results.next().shardedSQL;
            executeResults.add(ssql);
        }
    }

    @Test public void testShardingSQLSetAutoCommit0() throws FileNotFoundException {
        String originalsql = "set @@autocommiT =  0";
        ShardingSQL rewriter = handleSQL(originalsql, mixShardingRouter);
        assertEquals(rewriter.queryType, QUERY_TYPE.SET_AUTOCOMMIT);
        assertEquals(rewriter.autocommitValue, 0);
        assertEquals(rewriter.getOriginMostSafeSQL(), "SET @@autocommiT = 0");
    }

    @Test public void testShardingSQLSetAutoCommit1() throws FileNotFoundException {
        String originalsql = "set autocommiT =  1";
        ShardingSQL rewriter = handleSQL(originalsql, mixShardingRouter);
        assertEquals(rewriter.queryType, QUERY_TYPE.SET_AUTOCOMMIT);
        assertEquals(rewriter.autocommitValue, 1);
        assertEquals(rewriter.getOriginMostSafeSQL(), "SET autocommiT = 1");
    }

    @Test public void testShardingSQLInvalidSetAutoCommit1() throws FileNotFoundException {
        String originalsql = "set @autocommiT =  1";
        ShardingSQL rewriter = handleSQL(originalsql, mixShardingRouter);
        assertEquals(rewriter.queryType, QUERY_TYPE.IGNORE_CMD);
        assertEquals(rewriter.autocommitValue, -1);
        assertEquals(rewriter.getOriginMostSafeSQL(), "SET @autocommiT = ?");
    }

    @Test(expectedExceptions = Exception.class) public void testShardingSQLInvalidSetAutoCommit2()
        throws FileNotFoundException {
        String originalsql = "set @@autocommiT =  2";
        handleSQL(originalsql, mixShardingRouter);
    }

    @Test public void testSetAutoCommitOn() {
        String originalsql = "set autocommiT =  on";
        ShardingSQL rewriter = handleSQL(originalsql, mixShardingRouter);
        assertEquals(rewriter.queryType, QUERY_TYPE.SET_AUTOCOMMIT);
        assertEquals(rewriter.autocommitValue, 1);
        assertEquals(rewriter.getOriginMostSafeSQL(), "SET autocommiT = ON");
    }

    @Test public void testSetAutoCommitOff() {
        String originalsql = "set autocommiT =  off";
        ShardingSQL rewriter = handleSQL(originalsql, mixShardingRouter);
        assertEquals(rewriter.queryType, QUERY_TYPE.SET_AUTOCOMMIT);
        assertEquals(rewriter.autocommitValue, 0);
        assertEquals(rewriter.getOriginMostSafeSQL(), "SET autocommiT = off");
    }

    @Test public void testSetATATAutoCommitOff() {
        String originalsql = "set @@autocommiT =  off";
        ShardingSQL rewriter = handleSQL(originalsql, mixShardingRouter);
        assertEquals(rewriter.queryType, QUERY_TYPE.SET_AUTOCOMMIT);
        assertEquals(rewriter.autocommitValue, 0);
        assertEquals(rewriter.getOriginMostSafeSQL(), "SET @@autocommiT = off");
    }

    @Test public void testSingleDemistion1() throws FileNotFoundException {
        String originalsql = "select id from tb_shipping_order where tracking_id = 12345678";
        String changedsql1 = "SELECT id FROM tb_shipping_order_167 WHERE tracking_id = 12345678";
        positiveTest(originalsql, changedsql1, "", this.mixShardingRouter);
    }

    @Test public void testSingleDemistion1_2() throws FileNotFoundException {
        String originalsql = "select id from tb_shipping_order where user_id = 1023";
        ShardingSQL rewriter = handleSQL(originalsql, mixShardingRouter);
        while (rewriter.results.hasNext()) {
            String ssql = rewriter.results.next().shardedSQL;
            executeResults.add(ssql);
        }
        assertEquals(executeResults.size(), 512);
    }

    @Test public void testSingleDemistion2() throws FileNotFoundException {
        GreySwitch.getInstance().setShardingHashCheckLevel("pass");
        String originalsql =
            "insert into tb_shipping_order(restaurant_id,y,tracking_id) values(1023,2,12345678)";
        String changedsql1 = "/* E:" + Constants.ELE_META_BATCH_ANALYZED_MARKER
            + "=true:E */INSERT INTO tb_shipping_order_167 (restaurant_id, y, tracking_id) VALUES (1023, 2, 12345678)";
        positiveTest(originalsql, changedsql1, "", this.mixShardingRouter);
        GreySwitch.getInstance().setShardingHashCheckLevel("warn");
    }

    @Test public void testSingleDemistion3() throws FileNotFoundException {
        String originalsql =
            "update tb_shipping_order use index(primary) set x = 3 where retailer_id = 12345678";
        String changedsql1 =
            "UPDATE tb_shipping_order_66 USE INDEX (PRIMARY) SET x = 3 WHERE retailer_id = 12345678";
        positiveTest(originalsql, changedsql1, "", this.mixShardingRouter);
    }

    @Test public void testSingleDemistion4_alias() throws FileNotFoundException {
        String originalsql =
            "select `t1`.`id`, `t1`.`tracking_id` from `tb_shipping_order` as t1 where t1.`retailer_id` = 12345678";
        String changedsql1 =
            "SELECT `t1`.`id`, `t1`.`tracking_id` FROM tb_shipping_order_66 t1 WHERE t1.`retailer_id` = 12345678";
        positiveTest(originalsql, changedsql1, "", this.mixShardingRouter);
    }

    @Test public void testBindMasterExpr() {
        String originalsql =
            "select * from eos_mysql_task where task_hash = 1234567890 and 'bind_master' = 'bind_master' ";
        ShardingSQL rewriter = handleSQL(originalsql, mixShardingRouter);
        assertTrue(rewriter.isContainsBindMasterExpr);
        originalsql =
            "select * from abc where 'bind_master' = 'bind_master' or task_hash = 1234567890  ";
        rewriter = handleSQL(originalsql, mixShardingRouter);
        assertTrue(rewriter.isContainsBindMasterExpr);
        originalsql =
            "select * from abc where 'bind_master' = '1bind_master' or task_hash = 1234567890  ";
        rewriter = handleSQL(originalsql, mixShardingRouter);
        assertFalse(rewriter.isContainsBindMasterExpr);
    }

    @Test public void testOneKeyExpr() {
        String originalsql =
            "select * from abc where id = 1234567890 and 'bind_master' = 'bind_master' ";
        ShardingSQL rewriter = handleSQL(originalsql, mixShardingRouter);
        assertFalse(rewriter.sqlFeature.isOneKey());

        originalsql =
            "select id from abc where 'bind_master' = 'bind_master' or task_hash <= 1234567890  ";
        rewriter = handleSQL(originalsql, mixShardingRouter);
        assertFalse(rewriter.sqlFeature.isOneKey());

        originalsql = "select name from abc where 'bind_master' = 'bind_master'";
        rewriter = handleSQL(originalsql, mixShardingRouter);
        assertFalse(rewriter.sqlFeature.isOneKey());
        assertEquals(rewriter.sqlFeature.getTable(), "abc");

        originalsql = "update abc set name='a' where id > 5";
        rewriter = handleSQL(originalsql, mixShardingRouter);
        assertFalse(rewriter.sqlFeature.isOneKey());

        originalsql = "update abc set name='b' where id = 1";
        rewriter = handleSQL(originalsql, mixShardingRouter);
        assertTrue(rewriter.sqlFeature.isOneKey());

        originalsql = "delete from abc where id in (1,2,3,4,5)";
        rewriter = handleSQL(originalsql, mixShardingRouter);
        assertFalse(rewriter.sqlFeature.isOneKey());
        assertEquals(rewriter.sqlFeature.vendor, DBVendor.MYSQL);

        originalsql = "delete from abc where id between 1 and 5";
        rewriter = handleSQL(originalsql, mixShardingRouter);
        assertFalse(rewriter.sqlFeature.isOneKey());

        originalsql = "delete from abc where name = 'aaaaa'";
        rewriter = handleSQL(originalsql, mixShardingRouter);
        assertTrue(rewriter.sqlFeature.isOneKey());
        assertTrue(rewriter.sqlFeature.hasWhere());
    }

    @Test public void notInSQLFeature() {
        String sql =
            "select id, name from tb_shipping_order where tracking_id not in (1, 2, 4) order by name limit 3 offset 1";
        ShardingSQL shardingSQL = handleSQL(sql, mixShardingRouter);
        assertEquals(shardingSQL.sqlFeature.getSingleInValues(), -1l);
        assertEquals(shardingSQL.sqlFeature.getLimit(), 3l);
        assertEquals(shardingSQL.sqlFeature.getOffset(), 1l);
        assertTrue(shardingSQL.sqlFeature.hasOrderBy());
        assertFalse(shardingSQL.sqlFeature.hasGroupBy());
        assertTrue(shardingSQL.sqlFeature.isNeedSharding());
        assertTrue(shardingSQL.sqlFeature.isShardingSelectAll());
    }

    public void testMultiQuery1() {
        String originalsql = "select 1;select 2;select 3";
        ShardingSQL rewriter = handleSQL(originalsql, mixShardingRouter);
        assertEquals(rewriter.queryType, QUERY_TYPE.MULTI_NONE_TRANS);
        assertEquals(rewriter.originSQL, "SELECT 1;SELECT 2;SELECT 3");
        assertEquals(rewriter.mostSafeSQL, "SELECT ?;SELECT ?;SELECT ?");
        assertEquals(rewriter.tableName, "multiquery_table");

        originalsql = "select 1;insert INTO eleme_order (id) VALUES (1);select 3;";
        rewriter = handleSQL(originalsql, mixShardingRouter);
        assertEquals(rewriter.queryType, QUERY_TYPE.MULTI_TRANS);
        assertEquals(rewriter.originSQL,
            "SELECT 1;INSERT INTO eleme_order (id) VALUES (1);SELECT 3");
        assertEquals(rewriter.mostSafeSQL,
            "SELECT ?;INSERT INTO eleme_order (id) VALUES (?);SELECT ?");
        assertEquals(rewriter.tableName, "multiquery_table");

        originalsql = "select 1 from t1 for update;select 3;";
        rewriter = handleSQL(originalsql, mixShardingRouter);
        assertEquals(rewriter.queryType, QUERY_TYPE.MULTI_TRANS);
        assertEquals(rewriter.originSQL, "SELECT 1 FROM t1 FOR UPDATE;SELECT 3");
        assertEquals(rewriter.mostSafeSQL, "SELECT ? FROM t1 FOR UPDATE;SELECT ?");
        assertEquals(rewriter.tableName, "multiquery_table");
    }

    @Test public void testKillCmd1() throws Exception {
        String originalsql = "kill query 123";
        ShardingSQL rewriter = handleSQL(originalsql, mixShardingRouter);
        assertEquals(rewriter.getClientIdForKill(), 123);

        originalsql = "kill 123";
        rewriter = handleSQL(originalsql, mixShardingRouter);
        assertEquals(rewriter.getClientIdForKill(), 123);

        originalsql = "kill connection 123";
        rewriter = handleSQL(originalsql, mixShardingRouter);
        assertEquals(rewriter.getClientIdForKill(), 123);

        originalsql = "kill 123+1";
        rewriter = handleSQL(originalsql, mixShardingRouter);
        assertEquals(rewriter.getClientIdForKill(), -1);

        this.execute(() -> {
            handleSQL("kill abc", mixShardingRouter);
            fail();
        }, (e) -> {
            assertEquals(ErrorCode.ER_SYNTAX_ERROR, e.errorCode);
        });
    }

    @Test public void testMappingShardingInsert3() {
        // refer https://dev.mysql.com/doc/refman/8.0/en/string-literals.html
        // 对于单引号'的转义，使用反斜杠和2个单引号效果是一样的，druid统一使用2个单引号处理，最终写入到数据库的效果一致。
        String originalsql =
            "insert into tb_shipping_order (tracking_id,retailer_id,platform_tracking_id) values (100000041476640543,489543,'abc\\'dfhj')";
        String changedMappingSql2 =
            "INSERT INTO tb_shipping_order_tracking_id_mapping_dal_99 (platform_tracking_id, retailer_id) VALUES ('abc''dfhj', '489543') ON DUPLICATE KEY UPDATE retailer_id = '489543'";
        ShardingSQL rewriter = handleSQL(originalsql, mixShardingRouter);
        assertTrue(rewriter.sqlFeature.isNeedSharding());
        assertTrue(rewriter.mappingResults.hasNext());
        assertFalse(rewriter.results.hasNext());
        while (rewriter.mappingResults.hasNext()) {
            ShardingResult sr = rewriter.mappingResults.next();
            String ssql = sr.shardedSQL;
            executeResults.add(ssql);
        }
        expectResults.add(changedMappingSql2);
        assertEquals(executeResults, expectResults);
        assertEquals(rewriter.mostSafeSQL,
            "INSERT INTO tb_shipping_order_tracking_id_mapping_dal_ (platform_tracking_id, retailer_id) VALUES (?, ?) ON DUPLICATE KEY UPDATE retailer_id = ?");

        // 清理现场
        executeResults.clear();
        expectResults.clear();
        String changedSql = "/* E:" + Constants.ELE_META_BATCH_ANALYZED_MARKER
            + "=true:E */INSERT INTO tb_shipping_order_399 (tracking_id, retailer_id, platform_tracking_id) VALUES (100000041476640543, 489543, 'abc''dfhj')";
        assertFalse(rewriter.results.hasNext());
        rewriter.generateOriginalShardedSQLs();
        assertTrue(rewriter.results.hasNext());
        while (rewriter.results.hasNext()) {
            ShardingResult sr = rewriter.results.next();
            String ssql = sr.shardedSQL;
            executeResults.add(ssql);
        }
        expectResults.add(changedSql);
        assertEquals(executeResults, expectResults);
    }

    @Test public void testMappingShardingInsert4() {
        GreySwitch.getInstance().setShardingHashCheckLevel("pass");
        String originalsql =
            "insert into tb_shipping_order (tracking_id,retailer_id,platform_tracking_id) values (1028470,666,'😂''中文'),(1029495,667,'中文加😂')";
        String changedMappingSql1 =
            "INSERT INTO tb_shipping_order_tracking_id_mapping_dal_95 (platform_tracking_id, retailer_id) VALUES ('😂''中文', '666') ON DUPLICATE KEY UPDATE retailer_id = '666'";
        String changedMappingSql2 =
            "INSERT INTO tb_shipping_order_tracking_id_mapping_dal_101 (platform_tracking_id, retailer_id) VALUES ('中文加😂', '667') ON DUPLICATE KEY UPDATE retailer_id = '667'";
        ShardingSQL rewriter = handleSQL(originalsql, mixShardingRouter);
        assertTrue(rewriter.mappingResults.hasNext());
        assertFalse(rewriter.results.hasNext());
        while (rewriter.mappingResults.hasNext()) {
            ShardingResult sr = rewriter.mappingResults.next();
            String ssql = sr.shardedSQL;
            executeResults.add(ssql);
        }
        expectResults.add(changedMappingSql1);
        expectResults.add(changedMappingSql2);
        assertEquals(executeResults, expectResults);
        assertEquals(rewriter.mostSafeSQL,
            "INSERT INTO tb_shipping_order_tracking_id_mapping_dal_ (platform_tracking_id, retailer_id) VALUES (?, ?) ON DUPLICATE KEY UPDATE retailer_id = ?");

        // 清理现场
        executeResults.clear();
        expectResults.clear();
        String changedSql = "/* E:" + Constants.ELE_META_BATCH_ANALYZED_MARKER
            + "=true:E */INSERT INTO tb_shipping_order_187 (tracking_id, retailer_id, platform_tracking_id) VALUES (1028470, 666, '😂''中文'), (1029495, 667, '中文加😂')";
        assertFalse(rewriter.results.hasNext());
        rewriter.generateOriginalShardedSQLs();
        assertTrue(rewriter.results.hasNext());
        while (rewriter.results.hasNext()) {
            ShardingResult sr = rewriter.results.next();
            String ssql = sr.shardedSQL;
            executeResults.add(ssql);
        }
        expectResults.add(changedSql);
        assertEquals(executeResults, expectResults);
        GreySwitch.getInstance().setShardingHashCheckLevel("warn");
    }

    @Test public void insertShardingByComposeKeyMissMappingValue() {
        String originalsql =
            "insert into tb_shipping_order (tracking_id,platform_tracking_id) values (1028470,4146543412)";
        String changedSql =
            "INSERT INTO tb_shipping_order_187 (tracking_id, platform_tracking_id) VALUES (1028470, 4146543412)";
        positiveTest(originalsql, changedSql, "", mixShardingRouter, false);
    }

    @Test public void testMappingSelect1() {
        String originalsql =
            "select tracking_id,retailer_id,platform_tracking_id from tb_shipping_order where platform_tracking_id = 12345";
        String changedMappingSql2 =
            "SELECT retailer_id FROM tb_shipping_order_tracking_id_mapping_dal_25 WHERE platform_tracking_id = '12345'";
        ShardingSQL rewriter = handleSQL(originalsql, mixShardingRouter);
        assertTrue(rewriter.mappingResults.hasNext());
        assertFalse(rewriter.results.hasNext());
        while (rewriter.mappingResults.hasNext()) {
            ShardingResult sr = rewriter.mappingResults.next();
            String ssql = sr.shardedSQL;
            executeResults.add(ssql);
        }
        expectResults.add(changedMappingSql2);
        assertEquals(executeResults, expectResults);
        assertEquals(rewriter.mostSafeSQL,
            "SELECT retailer_id FROM tb_shipping_order_tracking_id_mapping_dal_ WHERE platform_tracking_id = ?");

        // 清理现场
        executeResults.clear();
        expectResults.clear();
        String changedSql =
            "SELECT tracking_id, retailer_id, platform_tracking_id FROM tb_shipping_order_399 WHERE platform_tracking_id = 12345";
        assertFalse(rewriter.results.hasNext());
        rewriter.collectShardingColumnValue(0, "489543");
        rewriter.generateOriginalShardedSQLs();
        assertTrue(rewriter.results.hasNext());
        while (rewriter.results.hasNext()) {
            ShardingResult sr = rewriter.results.next();
            String ssql = sr.shardedSQL;
            executeResults.add(ssql);
        }
        expectResults.add(changedSql);
        assertEquals(executeResults, expectResults);
    }

    /**
     * 测试映射表查询不到对应的映射记录时, sharding sql将发往最后一张sharding表
     */
    @Test public void testMappingSelect2() {
        String originalsql =
            "select tracking_id,retailer_id,platform_tracking_id from tb_shipping_order where platform_tracking_id = 12345 and platform_id = 1";
        ShardingSQL rewriter = handleSQL(originalsql, mixShardingRouter);
        assertFalse(rewriter.sqlFeature.isShardingSelectAll());
        String changedSql =
            "SELECT tracking_id, retailer_id, platform_tracking_id FROM tb_shipping_order_511 WHERE platform_tracking_id = 12345 AND platform_id = 1";
        assertFalse(rewriter.results.hasNext());
        rewriter.generateOriginalShardedSQLs();
        assertTrue(rewriter.results.hasNext());
        while (rewriter.results.hasNext()) {
            ShardingResult sr = rewriter.results.next();
            String ssql = sr.shardedSQL;
            executeResults.add(ssql);
        }
        expectResults.add(changedSql);
        assertEquals(executeResults, expectResults);
    }

    @Test public void testMappingSelect3() {
        String originalsql =
            "select tracking_id,retailer_id,platform_tracking_id from tb_shipping_order where platform_tracking_id in (214,123) and platform_tracking_id = 12345";
        ShardingSQL rewriter = handleSQL(originalsql, mixShardingRouter);
        String changedSql =
            "SELECT tracking_id, retailer_id, platform_tracking_id FROM tb_shipping_order_399 WHERE platform_tracking_id IN (214, 123) AND platform_tracking_id = 12345";
        assertFalse(rewriter.results.hasNext());
        rewriter.collectShardingColumnValue(0, "489543");
        rewriter.generateOriginalShardedSQLs();
        assertTrue(rewriter.results.hasNext());
        while (rewriter.results.hasNext()) {
            ShardingResult sr = rewriter.results.next();
            String ssql = sr.shardedSQL;
            executeResults.add(ssql);
        }
        expectResults.add(changedSql);
        assertEquals(executeResults, expectResults);
        assertEquals(rewriter.sqlFeature.getSingleInValues(), -1l);
        assertFalse(rewriter.sqlFeature.isOneKey());
    }

    /**
     * 主要测试sql中有别名和反引号的情况下依然正常工作
     */
    @Test public void testMappingSelect4() {
        String originalsql =
            "select tb_shipping_order.`tracking_id` as `tracking_id`, tb_shipping_order.`retailer_id` as `retailer_id`,"
                + "tb_shipping_order.`platform_tracking_id` pt_id from apollo.`tb_shipping_order` where pt_id = 346";
        ShardingSQL rewriter = handleSQL(originalsql, mixShardingRouter);
        String changedMappingSql2 =
            "SELECT retailer_id FROM tb_shipping_order_tracking_id_mapping_dal_250 WHERE platform_tracking_id = '346'";
        assertTrue(rewriter.mappingResults.hasNext());
        assertFalse(rewriter.results.hasNext());
        while (rewriter.mappingResults.hasNext()) {
            ShardingResult sr = rewriter.mappingResults.next();
            String ssql = sr.shardedSQL;
            executeResults.add(ssql);
        }
        expectResults.add(changedMappingSql2);
        assertEquals(executeResults, expectResults);

        // 清理现场
        executeResults.clear();
        expectResults.clear();
        String changedSql =
            "SELECT tb_shipping_order_399.`tracking_id` AS `tracking_id`, tb_shipping_order_399.`retailer_id` AS `retailer_id`,"
                + " tb_shipping_order_399.`platform_tracking_id` AS pt_id FROM tb_shipping_order_399 WHERE pt_id = 346";
        assertFalse(rewriter.results.hasNext());
        rewriter.collectShardingColumnValue(0, "489543");
        rewriter.generateOriginalShardedSQLs();
        assertTrue(rewriter.results.hasNext());
        while (rewriter.results.hasNext()) {
            ShardingResult sr = rewriter.results.next();
            String ssql = sr.shardedSQL;
            executeResults.add(ssql);
        }
        expectResults.add(changedSql);
        assertEquals(executeResults, expectResults);
    }

    /**
     * insert 类型的sharding暂时不支持batch insert, 以后支持的话请删除或者修改此测试
     */
    // @Test
    // public void testBatchInsert() {
    // String originalsql = "insert into tb_shipping_order (tracking_id,retailer_id,platform_tracking_id) values (100000041476640543,489543,12345), "
    // + "(100000041476640543,489542,12344)";
    // try {
    // handleSQL(originalsql, mixShardingRouter);
    // fail();
    // } catch (QueryException e) {
    // assertTrue(e.getMessage().contains("multi rows insert is invalid"));
    // }
    // }
    @Test public void testParseFakeSql() {
        Map<String, String> expectedParams = new HashMap<>();
        String help = "SELECT help FROM dal_dual";
        String tables = "SELECT tables FROM dal_dual";
        String kill = "SELECT killctx FROM dal_dual WHERE value = 123";
        String etrace_sql_pattern = "SELECT etrace_sql_pattern FROM dal_dual WHERE value = 999";
        String unsafe_on = "SELECT unsafe_on FROM dal_dual";
        String unsafe_off = "SELECT unsafe_off FROM dal_dual ";
        String globalid_order_id =
            "SELECT next_value FROM dal_dual WHERE seq_name='order_id' AND user_id='123' AND restaurant_id='456'";
        String sharding_count =
            "SELECT sharding_count FROM dal_dual where table_name='eleme_order'";
        String globalid_common_seq =
            "SELECT next_value FROM dal_dual WHERE seq_name = 'common_seq' AND biz = 'hongbao'";
        String globalid_composed_seq =
            "SELECT next_value FROM dal_dual WHERE seq_name = 'composed_seq' AND biz = 'moses' AND org_id = '123' AND team_id = '456'";
        ShardingSQL rewriter = handleSQL(help, mixShardingRouter);
        assertEquals(rewriter.selected_value, "help");
        assertTrue(rewriter.params.isEmpty());

        rewriter = handleSQL(tables, mixShardingRouter);
        assertEquals(rewriter.selected_value, "tables");
        assertTrue(rewriter.params.isEmpty());

        rewriter = handleSQL(kill, mixShardingRouter);
        assertEquals(rewriter.selected_value, "killctx");
        expectedParams.put("value", "123");
        assertEquals(rewriter.params, expectedParams);

        expectedParams.clear();
        rewriter = handleSQL(etrace_sql_pattern, mixShardingRouter);
        assertEquals(rewriter.selected_value, "etrace_sql_pattern");
        expectedParams.put("value", "999");
        assertEquals(rewriter.params, expectedParams);

        expectedParams.clear();
        rewriter = handleSQL(unsafe_on, mixShardingRouter);
        assertEquals(rewriter.selected_value, "unsafe_on");
        assertTrue(rewriter.params.isEmpty());

        expectedParams.clear();
        rewriter = handleSQL(unsafe_off, mixShardingRouter);
        assertEquals(rewriter.selected_value, "unsafe_off");
        assertTrue(rewriter.params.isEmpty());

        expectedParams.clear();
        rewriter = handleSQL(globalid_order_id, mixShardingRouter);
        assertEquals(rewriter.selected_value, "next_value");
        expectedParams.put("seq_name", "order_id");
        expectedParams.put("user_id", "123");
        expectedParams.put("restaurant_id", "456");
        assertEquals(rewriter.params, expectedParams);

        expectedParams.clear();
        rewriter = handleSQL(sharding_count, mixShardingRouter);
        assertEquals(rewriter.selected_value, "sharding_count");
        expectedParams.put("table_name", "eleme_order");
        assertEquals(rewriter.params, expectedParams);

        expectedParams.clear();
        rewriter = handleSQL(globalid_common_seq, mixShardingRouter);
        assertEquals(rewriter.selected_value, "next_value");
        expectedParams.put("seq_name", "common_seq");
        expectedParams.put("biz", "hongbao");
        assertEquals(rewriter.params, expectedParams);

        expectedParams.clear();
        rewriter = handleSQL(globalid_composed_seq, mixShardingRouter);
        assertEquals(rewriter.selected_value, "next_value");
        expectedParams.put("seq_name", "composed_seq");
        expectedParams.put("biz", "moses");
        expectedParams.put("org_id", "123");
        expectedParams.put("team_id", "456");
        assertEquals(rewriter.params, expectedParams);
    }

    @Test public void testShowStatement() throws IOException {
        AllInOneMonitor.getInstance()
            .loadConfig(getClass().getClassLoader().getResource("db-connections.cfg").getFile());
        DBChannelDispatcher.getHolders().values().forEach(dispathcer -> {
            dispathcer.getDalGroupConfig()
                .setDbInfos(AllInOneMonitor.getInstance().getDbConnectionInfos());
            dispathcer.updateDbInfo(AllInOneMonitor.getInstance().getDbConnectionInfos());
        });
        ShardingSQL rewriter = handleSQL("show tables from eos", mixShardingRouter);
        assertEquals(rewriter.originSQL, "SHOW TABLES FROM `eleme`");

        rewriter =
            handleSQL("show tables from `napos_operation-zeus_ers_group`", mixShardingRouter);
        assertEquals(rewriter.originSQL, "SHOW TABLES FROM `napos_db`");

        rewriter =
            handleSQL("show columns from eos.eleme_order_no_sharding_table", mixShardingRouter);
        assertEquals(rewriter.originSQL, "SHOW COLUMNS FROM `eleme`.eleme_order_no_sharding_table");

        rewriter =
            handleSQL("show index from eos.eleme_order_no_sharding_table", mixShardingRouter);
        assertEquals(rewriter.originSQL, "SHOW INDEX FROM `eleme`.eleme_order_no_sharding_table");

        rewriter = handleSQL("show keys from eos.eleme_order_no_sharding_table", mixShardingRouter);
        assertEquals(rewriter.originSQL, "SHOW KEYS FROM `eleme`.eleme_order_no_sharding_table");

        rewriter = handleSQL("show create database eos", mixShardingRouter);
        assertEquals(rewriter.originSQL, "SHOW CREATE DATABASE `eleme`");

        rewriter = handleSQL("show open tables from  eos", mixShardingRouter);
        assertEquals(rewriter.originSQL, "SHOW OPEN TABLES FROM `eleme`");

        rewriter = handleSQL("show table status from eos", mixShardingRouter);
        assertEquals(rewriter.originSQL, "SHOW TABLE STATUS FROM `eleme`");
        rewriter = handleSQL("show triggers from  eos", mixShardingRouter);
        assertEquals(rewriter.originSQL, "SHOW TRIGGERS FROM `eleme`");
    }

    @Test public void testShowCreateTable() throws IOException {
        String originalSql = "show create table tb_shipping_order";
        String changedsql1 = "SHOW CREATE TABLE tb_shipping_order_511";
        String dbName = "apollo_group4";
        showTest(originalSql, changedsql1, dbName);

        originalSql = "show create table tb_shipping_order_65";
        changedsql1 = "SHOW CREATE TABLE tb_shipping_order_65";
        dbName = "apollo_group1";
        showTest(originalSql, changedsql1, dbName);
    }

    @Test public void testShowFullcolumnsFromTable() throws IOException {
        AllInOneMonitor.getInstance().loadConfig("src/test/db-connections.cfg");
        DBChannelDispatcher.getHolders().values().forEach(dispathcer -> {
            dispathcer.getDalGroupConfig()
                .setDbInfos(AllInOneMonitor.getInstance().getDbConnectionInfos());
            dispathcer.updateDbInfo(AllInOneMonitor.getInstance().getDbConnectionInfos());
        });
        String originalSql = "show columns from tb_shipping_order from `apollo_group`";
        String changedsql1 = "SHOW COLUMNS FROM tb_shipping_order_511";
        String dbName = "apollo_group4";
        showTest(originalSql, changedsql1, dbName);

        originalSql = "show columns from tb_shipping_order_65 from `apollo_group`";
        changedsql1 = "SHOW COLUMNS FROM tb_shipping_order_65";
        dbName = "apollo_group1";
        showTest(originalSql, changedsql1, dbName);

        originalSql = "show full columns from tb_shipping_order";
        changedsql1 = "SHOW FULL COLUMNS FROM tb_shipping_order_511";
        dbName = "apollo_group4";
        showTest(originalSql, changedsql1, dbName);

        originalSql = "show keys from tb_shipping_order from `apollo_group`";
        changedsql1 = "SHOW KEYS FROM tb_shipping_order_511";
        dbName = "apollo_group4";
        showTest(originalSql, changedsql1, dbName);

        originalSql = "show keys from tb_shipping_order";
        changedsql1 = "SHOW KEYS FROM tb_shipping_order_511";
        dbName = "apollo_group4";
        showTest(originalSql, changedsql1, dbName);

        originalSql = "show index from tb_shipping_order";
        changedsql1 = "SHOW INDEX FROM tb_shipping_order_511";
        dbName = "apollo_group4";
        showTest(originalSql, changedsql1, dbName);

        originalSql = "show index from tb_shipping_order from `apollo_group`";
        changedsql1 = "SHOW INDEX FROM tb_shipping_order_511";
        dbName = "apollo_group4";
        showTest(originalSql, changedsql1, dbName);
    }

    /**
     * {@literal SafeMySQLOutputVisitor#visit(SQLInListExpr) }
     *
     * @see SafeMySQLOutputVisitor#visit(SQLInListExpr)
     */
    @Test public void testSelectManyInExpr() throws FileNotFoundException {
        String originalsql = "select * from some_table where x in ('1','1','1','1','1','1','1')";
        ShardingSQL rewriter = handleSQL(originalsql, ShardingRouter.defaultShardingRouter);
        assertEquals(rewriter.mostSafeSQL, "SELECT * FROM some_table WHERE x IN (?)");
        assertEquals(rewriter.sqlFeature.getSingleInValues(), 7l);
    }

    @Test public void testTupleInExpr() throws Exception {
        String sql =
            "SELECT id FROM t_wms_stock WHERE (warehouse_id, material_id) IN ((1, 2), (3,4), (5,6),(7,8))";
        ShardingSQL shardingSQL = handleSQL(sql, ShardingRouter.defaultShardingRouter);
        assertEquals(shardingSQL.getOriginMostSafeSQL(),
            "SELECT id FROM t_wms_stock WHERE (warehouse_id, material_id) IN (?)");
        assertEquals(shardingSQL.sqlFeature.getSingleInValues(), 4l);
    }

    @Test public void testFunctionInExpr() throws Exception {
        String sql =
            "SELECT shop_id, check_time_id, user_id FROM t_assistant_work_feedback WHERE work_date IN (DATE('2017-04-19 21:40:21'), DATE('2017-04-19 21:40:21'), DATE('2018-01-10 21:40:21'),DATE('2018-01-09 21:40:21'), DATE('2017-12-20 21:40:21'), DATE('2017-12-31 21:40:21'))";
        ShardingSQL shardingSQL = handleSQL(sql, ShardingRouter.defaultShardingRouter);
        assertEquals(shardingSQL.getOriginMostSafeSQL(),
            "SELECT shop_id, check_time_id, user_id FROM t_assistant_work_feedback WHERE work_date IN (?)");
        assertEquals(shardingSQL.sqlFeature.getSingleInValues(), 6l);
    }

    @Test public void testSelectLimitSafeSQL() throws FileNotFoundException {
        String originalsql = "select * from some_table where x = 100 limit 2,3";
        ShardingSQL rewriter = handleSQL(originalsql, ShardingRouter.defaultShardingRouter);
        assertEquals(rewriter.getWhiteFields(), "LIMIT 3 OFFSET 2");
    }

    @Test public void testShardingInsertSql01() throws QueryException {
        String originalsql = "insert into tb_shipping_order (tracking_id) values (1), (200)";
        this.execute(() -> {
            String queryComment = "";
            Send2BatchCond batchCond = new Send2BatchCond().setBatchSendComment(queryComment);
            ShardingSQL shardingSQL = ShardingSQL
                .handleMySQLBasicly(originalsql, queryComment, mixShardingRouter, batchCond,
                    whiteFieldsFilter);
            ShardingSQL.handleSQLSharding(shardingSQL);
            fail();
        }, (e) -> assertTrue(
            e.toString().contains("insert multi rows with different sharding id is not allowed")));
    }

    @Test public void testShardingInsertSql02() {
        GreySwitch.getInstance().setShardingHashCheckLevel("pass");
        String originalsql =
            "insert into tb_shipping_order (tracking_id, retailer_id) values (1000,1), (1000,2)";
        ShardingSQL shardingSQL = handleSQL(originalsql, mixShardingRouter);
        assertEquals(shardingSQL.results.count, 1);

        String testSql = "insert into tb_shipping_order (tracking_id) values (1), (2)";
        ShardingSQL shardingSQL1 = handleSQL(testSql, mixShardingRouter);
        assertEquals(shardingSQL1.results.count, 1);
        GreySwitch.getInstance().setShardingHashCheckLevel("warn");
    }


    /*
     * 测试insert时composedkey与shardingkey是否在同一分片
     * case: composedKey:tracking_id
     *       shardingkey:retailer_id
     *       generator:composed_seq
     * */
    @Test public void testShardingHashInsertSql01() throws SeqsException {
        GreySwitch.getInstance().setShardingHashCheckLevel("reject");
        Map<String, String> params = new HashMap<>();
        params.put("seq_name", GeneratorUtil.COMPOSED_SEQ);
        params.put("retailer_id", "2455");
        ComposedGenerator orderIdGenerator = new ComposedGenerator();
        Long calc = Long.valueOf(orderIdGenerator.calc(122455, params));

        String originalsql = String
            .format("insert into tb_shipping_order (retailer_id, tracking_id) values (%d, %d)",
                2455, calc);
        ShardingSQL shardingSQL = handleSQL(originalsql, shardingRouterNewApollo);
        assertEquals(shardingSQL.results.count, 1);

        originalsql =
            String.format("insert into tb_shipping_order (tracking_id) values (%d)", calc);
        shardingSQL = handleSQL(originalsql, shardingRouterNewApollo);
        assertEquals(shardingSQL.results.count, 1);

        //开启reject后会报错
        try {
            originalsql = String
                .format("insert into tb_shipping_order (retailer_id, tracking_id) values (%d, %d)",
                    2456, calc);
            shardingSQL = handleSQL(originalsql, shardingRouterNewApollo);
            assertEquals(1, 2);
        } catch (Exception e) {
            assertEquals(e.getMessage(),
                "Invalid sharding SQL: insert into tb_shipping_order (retailer_id, tracking_id) values (2456, 125394018) , errorMessage: insert data composedkey not match shardingkey, tableName : tb_shipping_order, (column, inputValue, shardingHash) (retailer_id , 2456 , 99)(tracking_id , 125394018 , 98)");
        }

        //开启pass后通过
        GreySwitch.getInstance().setShardingHashCheckLevel("pass");
        originalsql = String
            .format("insert into tb_shipping_order (retailer_id, tracking_id) values (%d, %d)",
                2456, calc);
        shardingSQL = handleSQL(originalsql, shardingRouterNewApollo);
        assertEquals(shardingSQL.results.count, 1);

        GreySwitch.getInstance().setShardingHashCheckLevel("warn");
    }

    /*
     * case:  composedkey:id
     *        shardingkey:id
     * */
    @Test public void testShardingHashInsertSql03() {
        GreySwitch.getInstance().setShardingHashCheckLevel("reject");
        String originalsql = "insert into tb_shipping_order (tracking_id) values (100),(101)";
        ShardingSQL shardingSQL = handleSQL(originalsql, mixShardingRouter);
        assertEquals(shardingSQL.results.count, 1);
        GreySwitch.getInstance().setShardingHashCheckLevel("warn");
    }

    @Test public void testMappingShardingUpdateDeleteByConditionInSucc() {
        String update =
            "UPDATE tb_shipping_order SET platform_name='test multi update' WHERE platform_tracking_id IN ('34234', '3245723985')";
        ShardingSQL updateShardingSQL = handleSQL(update, mixShardingRouter);
        assertTrue(updateShardingSQL.mappingResults.count == 2);
        updateShardingSQL.collectShardingColumnValue(0, "123");
        updateShardingSQL.collectShardingColumnValue(0, "123");
        updateShardingSQL.generateOriginalShardedSQLs();
        String expectedUpdateShardedSql =
            "UPDATE tb_shipping_order_281 SET platform_name = 'test multi update' WHERE platform_tracking_id IN ('34234', '3245723985')";
        assertEquals(updateShardingSQL.results.next().shardedSQL, expectedUpdateShardedSql);

        String delete =
            "DELETE FROM tb_shipping_order WHERE platform_tracking_id IN ('34234', '3245723985')";
        ShardingSQL deleteShardingSQL = handleSQL(delete, mixShardingRouter);
        assertTrue(deleteShardingSQL.mappingResults.count == 2);
        deleteShardingSQL.collectShardingColumnValue(0, "123");
        deleteShardingSQL.collectShardingColumnValue(0, "123");
        deleteShardingSQL.generateOriginalShardedSQLs();
        String expectedDeleteShardedSql =
            "DELETE FROM tb_shipping_order_281 WHERE platform_tracking_id IN ('34234', '3245723985')";
        assertTrue(deleteShardingSQL.results.next().shardedSQL.endsWith(expectedDeleteShardedSql));
    }

    @Test(expectedExceptions = QueryException.class, expectedExceptionsMessageRegExp = ".*DELETE multi rows in different sharding id is not allowed.*")
    public void testMappingShardingUpdateDeleteByConditionInFail() {
        String delete =
            "DELETE FROM tb_shipping_order WHERE platform_tracking_id IN ('34234', '3245723985')";
        ShardingSQL deleteShardingSQL = handleSQL(delete, mixShardingRouter);
        assertTrue(deleteShardingSQL.mappingResults.count == 2);
        deleteShardingSQL.collectShardingColumnValue(0, "123");
        deleteShardingSQL.collectShardingColumnValue(0, "124");
        deleteShardingSQL.generateOriginalShardedSQLs();
    }

    @Test public void testOneDimShardingUpdateDeleteByConditionInSucc() {
        String update =
            "UPDATE tb_shipping_order SET description='test multi update' WHERE tracking_id IN (4097586, 4098610)";
        ShardingSQL updateShardingSQL = handleSQL(update, mixShardingRouter);
        String expectedUpdateShardedSql =
            "UPDATE tb_shipping_order_281 SET description = 'test multi update' WHERE tracking_id IN (4097586, 4098610)";
        assertEquals(updateShardingSQL.results.next().shardedSQL, expectedUpdateShardedSql);

        String delete = "DELETE FROM tb_shipping_order WHERE tracking_id IN (4097586, 4098610)";
        ShardingSQL deleteShardingSQL = handleSQL(delete, mixShardingRouter);
        String expectedDeleteShardedSql =
            "DELETE FROM tb_shipping_order_281 WHERE tracking_id IN (4097586, 4098610)";
        assertTrue(deleteShardingSQL.results.next().shardedSQL.endsWith(expectedDeleteShardedSql));
    }

    @Test(expectedExceptions = QueryException.class, expectedExceptionsMessageRegExp = ".*DELETE multi rows in different sharding id is not allowed.*")
    public void testOneDimShardingDeleteByConditionInFail() {
        String delete =
            "DELETE FROM tb_shipping_order WHERE tracking_id IN (4097586, 4098610, 5124530)";
        handleSQL(delete, mixShardingRouter);
    }

    @Test public void testOneDimShardingDeleteByShardingKeyConditionInSucc() {
        String delete = "delete from tb_shipping_order where retailer_id in (123, 244)";
        ShardingSQL deleteShardingSql = handleSQL(delete, mixShardingRouter);
        String expectedDeleteShardedSql =
            "DELETE FROM tb_shipping_order_281 WHERE retailer_id IN (123, 244)";
        assertTrue(deleteShardingSql.results.next().shardedSQL.endsWith(expectedDeleteShardedSql));
    }

    @Test(expectedExceptions = QueryException.class, expectedExceptionsMessageRegExp = ".*DELETE multi rows in different sharding id is not allowed.*")
    public void testOneDimShardingDeleteByShardingKeyConditionInFail() {
        String delete = "delete from tb_shipping_order where retailer_id in (123, 244, 124)";
        handleSQL(delete, mixShardingRouter);
    }

    @Test public void testOneDimMultiInsertOk() {
        String sql =
            "INSERT INTO tb_shipping_order(tracking_id, retailer_id) VALUES(6145458, 321),(6146482, 321)";
        ShardingSQL shardingSQL = ShardingSQL
            .handleMySQLBasicly(sql, "", mixShardingRouter, Send2BatchCond.getDefault(),
                whiteFieldsFilter);
        ShardingSQL.handleSQLSharding(shardingSQL);
        String expectedPrefix =
            "INSERT INTO tb_shipping_order_217 (tracking_id, retailer_id) VALUES";
        assertTrue(shardingSQL.results.next().shardedSQL.startsWith(expectedPrefix));
    }

    @Test(expectedExceptions = QueryException.class, expectedExceptionsMessageRegExp = ".*insert multi rows with different sharding id is not allowed.*")
    public void testOneDimMultiInsertErr() {
        String sql =
            "INSERT INTO tb_shipping_order(tracking_id, retailer_id) VALUES(6145458, 321),(6147647, 21)";
        ShardingSQL shardingSQL = ShardingSQL
            .handleMySQLBasicly(sql, "", mixShardingRouter, Send2BatchCond.getDefault(),
                whiteFieldsFilter);
        ShardingSQL.handleSQLSharding(shardingSQL);
    }

    @Test public void testPatternWithOutComment() {
        String sql =
            "/* E:rid=nevermore.api^^48c8b3b4-2cde-4af1-a241-76dad3de3b16|1486707900748&rpcid=zeus.eos|1.5.1.1&role=slave:E */select 1";
        ShardingSQL shardingSQL = handleSQL(sql, mixShardingRouter);
        assertEquals(shardingSQL.getMostSafeSQL(), "SELECT ?");
    }

    @Test public void testMysqlStartTransaction() {
        String sql =
            "/* E:rid=nevermore.api^^48c8b3b4-2cde-4af1-a241-76dad3de3b16|1486707900748&rpcid=zeus.eos|1.5.1.1&role=slave:E */start transaction";
        ShardingSQL shardingSQL = handleSQL(sql, mixShardingRouter);
        assertEquals(shardingSQL.getOriginMostSafeSQL(), "START TRANSACTION");
        assertEquals(shardingSQL.queryType, QUERY_TYPE.BEGIN);
    }

    @Test public void testQueryAutocommit1() {
        String sql = "select @@autoCommit";
        ShardingSQL shardingSQL = handleSQL(sql, mixShardingRouter);
        assertEquals(shardingSQL.queryType, QUERY_TYPE.QUERY_AUTOCOMMIT);
        assertEquals(shardingSQL.getQueryAutoCommitHeader(), "@@autoCommit");
        assertFalse(shardingSQL.isQueryGlobalAutoCommit());
    }

    @Test public void testQueryAutocommit2() {
        String sql = "select @@global.autoCommit";
        ShardingSQL shardingSQL = handleSQL(sql, mixShardingRouter);
        assertEquals(shardingSQL.queryType, QUERY_TYPE.QUERY_AUTOCOMMIT);
        assertEquals(shardingSQL.getQueryAutoCommitHeader(), "@@global.autoCommit");
        assertTrue(shardingSQL.isQueryGlobalAutoCommit());
    }

    @Test public void testQueryAutocommit3() {
        String sql = "select @@Session.autoCommit";
        ShardingSQL shardingSQL = handleSQL(sql, mixShardingRouter);
        assertEquals(shardingSQL.queryType, QUERY_TYPE.QUERY_AUTOCOMMIT);
        assertEquals(shardingSQL.getQueryAutoCommitHeader(), "@@Session.autoCommit");
        assertFalse(shardingSQL.isQueryGlobalAutoCommit());
    }

    @Test public void testQueryAutocommit4() {
        String sql = "select @@Session.autoCommit as autocommit";
        ShardingSQL shardingSQL = handleSQL(sql, mixShardingRouter);
        assertEquals(shardingSQL.queryType, QUERY_TYPE.QUERY_AUTOCOMMIT);
        assertEquals(shardingSQL.getQueryAutoCommitHeader(), "autocommit");
        assertFalse(shardingSQL.isQueryGlobalAutoCommit());
    }

    @Test public void testAccurateShardInConditionWhenShardingIndexHitMissByShardingKey() {
        String inValues =
            IntStream.range(0, 100).mapToObj(String::valueOf).collect(Collectors.joining(", "));
        String originalsql =
            "SELECT restaurant_id as id1, retailer_id as id2 FROM tb_shipping_order WHERE tb_shipping_order.id1 in (1, 101) and sharding_index = 3 and tb_shipping_order.id2 in ("
                + inValues + ")";
        expectResults.add(
            "SELECT restaurant_id AS id1, retailer_id AS id2 FROM tb_shipping_order_3 WHERE tb_shipping_order_3.id1 IN (1, 101) AND 'sharding_index' = 'sharding_index' AND tb_shipping_order_3.id2 IN (0)");
        positiveTest(originalsql, expectResults, mixShardingRouter);
    }

    @Test public void testAccurateShardInConditionWhenShardingIndexHitMissByComposeKey() {
        String inValues = IntStream.range(10241, 10341).mapToObj(String::valueOf)
            .collect(Collectors.joining(", "));
        String originalsql =
            "SELECT tracking_id as id0, retailer_id as id1, user_id as id2 FROM tb_shipping_order WHERE tb_shipping_order.id1 in (1, 101) and sharding_index = 3 and tb_shipping_order.id0 in ("
                + inValues + ")";
        expectResults.add(
            "SELECT tracking_id AS id0, retailer_id AS id1, user_id AS id2 FROM tb_shipping_order_3 WHERE tb_shipping_order_3.id1 IN (1, 101) AND 'sharding_index' = 'sharding_index' AND tb_shipping_order_3.id0 IN (10241)");
        positiveTest(originalsql, expectResults, mixShardingRouter);
    }

    @Test public void testAccurateShardInConditionWhenShardingIndexHitted() {
        String inValues =
            IntStream.range(0, 100).mapToObj(String::valueOf).collect(Collectors.joining(", "));
        String originalsql =
            "SELECT retailer_id as id1, tracking_id as id2 FROM tb_shipping_order WHERE tb_shipping_order.id1 in (1, 101) and sharding_index = 5 and tb_shipping_order.id2 in ("
                + inValues + ")";
        expectResults.add(
            "SELECT retailer_id AS id1, tracking_id AS id2 FROM tb_shipping_order_5 WHERE tb_shipping_order_5.id1 IN (1, 101) AND 'sharding_index' = 'sharding_index' AND tb_shipping_order_5.id2 IN (0)");
        positiveTest(originalsql, expectResults, mixShardingRouter);
    }

    @Test public void testNoAccurateShardInConditionWhenNoShardingColumnIn() {
        String inValues =
            IntStream.range(0, 100).mapToObj(String::valueOf).collect(Collectors.joining(", "));
        String originalsql =
            "SELECT retailer_id as id1, no_shard_col FROM tb_shipping_order WHERE tb_shipping_order.id1 = 101 and tb_shipping_order.no_shard_col in ("
                + inValues + ")";
        expectResults.add(
            "SELECT retailer_id AS id1, no_shard_col FROM tb_shipping_order_249 WHERE tb_shipping_order_249.id1 = 101 AND tb_shipping_order_249.no_shard_col IN ("
                + inValues + ")");
        positiveTest(originalsql, expectResults, mixShardingRouter);
    }

    @Test public void testHackInReshardCondtion4UpdateWithoutWhere() {
        String sql =
            "/* E:rid=nevermore.api^^48c8b3b4-2cde-4af1-a241-76dad3de3b16|1486707900748&rpcid=zeus.eos|1.5.1.1&role=slave:E */update test set a=1";
        ShardingSQL shardingSQL = handleSQL(sql, mixShardingRouter);
        shardingSQL.rewriteSQL4ReBalance("2017-02-27T07:07:47.111");
        String expectedOriginSQL =
            "UPDATE test SET a = 1 WHERE created_at > '2017-02-27T07:07:47.111'";
        assertEquals(shardingSQL.originSQL, expectedOriginSQL);
    }

    @Test public void testHackInReshardCondtion4UpdateWithWhere() {
        String sql =
            "/* E:rid=nevermore.api^^48c8b3b4-2cde-4af1-a241-76dad3de3b16|1486707900748&rpcid=zeus.eos|1.5.1.1&role=slave:E */update test set a=1 where b = 'test' and c > 3";
        ShardingSQL shardingSQL = handleSQL(sql, mixShardingRouter);
        shardingSQL.rewriteSQL4ReBalance("2017-02-27T07:07:47.111");
        String expectedOriginSQL =
            "UPDATE test SET a = 1 WHERE created_at > '2017-02-27T07:07:47.111' AND (b = 'test' AND c > 3)";
        assertEquals(shardingSQL.originSQL, expectedOriginSQL);
    }

    @Test public void testShardingHackInReshardCondition4UpdateWithWhere() {
        //sharding情况下，update必须有where条件节点，否则会报错。
        String update =
            "UPDATE tb_shipping_order SET description = 'test multi update' WHERE retailer_id IN (1050200533)";
        ShardingSQL updateShardingSQL = handleSQL(update, mixShardingRouter);
        updateShardingSQL.rewriteSQL4ReBalance("2017-02-27T07:07:47.111");
        String expectedUiddSql =
            "UPDATE tb_shipping_order_494 SET description = 'test multi update' WHERE created_at > '2017-02-27T07:07:47.111' AND retailer_id IN (1050200533)";
        assertEquals(updateShardingSQL.results.next().shardedSQL, expectedUiddSql);
        String expectedOriginSQL =
            "UPDATE tb_shipping_order SET description = 'test multi update' WHERE created_at > '2017-02-27T07:07:47.111' AND retailer_id IN (1050200533)";
        assertEquals(updateShardingSQL.originSQL, expectedOriginSQL);
        String expectedOriginMostSafeSQL =
            "UPDATE tb_shipping_order SET description = ? WHERE created_at > ? AND retailer_id IN (?)";
        assertEquals(updateShardingSQL.getOriginMostSafeSQL(), expectedOriginMostSafeSQL);
    }

    @Test public void testShardingIndexHackInReshardCondition4UpdateWithWhere() {
        //sharding情况下，update必须有where条件节点，否则会报错。
        String update =
            "UPDATE tb_shipping_order SET description = 'test update' WHERE sharding_index=1";
        ShardingSQL updateShardingSQL = handleSQL(update, mixShardingRouter);
        updateShardingSQL.rewriteSQL4ReBalance("2017-02-27T07:07:47.111");
        String expectedUiddSql =
            "UPDATE tb_shipping_order_1 SET description = 'test update' WHERE created_at > '2017-02-27T07:07:47.111' AND 'sharding_index' = 'sharding_index'";
        assertEquals(updateShardingSQL.results.next().shardedSQL, expectedUiddSql);
        String expectedOriginSQL =
            "UPDATE tb_shipping_order SET description = 'test update' WHERE created_at > '2017-02-27T07:07:47.111' AND 'sharding_index' = 'sharding_index'";
        assertEquals(updateShardingSQL.originSQL, expectedOriginSQL);
        String expectedOriginMostSafeSQL =
            "UPDATE tb_shipping_order SET description = ? WHERE created_at > ? AND ? = ?";
        assertEquals(updateShardingSQL.getOriginMostSafeSQL(), expectedOriginMostSafeSQL);
    }

    @Test public void testShardingKeepIndexHints() {
        String sql =
            "SELECT id FROM tb_shipping_order USE INDEX (created_at) WHERE created_at < '2017-10-01' AND created_at >= '2015-03-01' AND tb_shipping_order.user_id = 721864719 AND sharding_index = 3 LIMIT 10";
        ShardingSQL selectShardingSQL = handleSQL(sql, mixShardingRouter);
        String expectedShardedSQL =
            "SELECT id FROM tb_shipping_order_3 USE INDEX (created_at) WHERE created_at < '2017-10-01' AND created_at >= '2015-03-01' AND tb_shipping_order_3.user_id = 721864719 AND 'sharding_index' = 'sharding_index' LIMIT 10";
        assertEquals(selectShardingSQL.results.next().shardedSQL, expectedShardedSQL);
    }

    @Test public void testRewriteIndexHintsForSharding() {
        String sql =
            "SELECT id FROM tb_shipping_order WHERE created_at < '2017-10-01' AND created_at >= '2015-03-01' AND tb_shipping_order.user_id = 721864719 AND sharding_index = 3 LIMIT 10";
        ShardingSQL selectShardingSQL = handleSQL(sql, mixShardingRouter);
        MySqlUseIndexHint hint = new MySqlUseIndexHint();
        hint.setIndexList(Collections.singletonList(new SQLIdentifierExpr("created_at")));
        selectShardingSQL.rewriteSQLHints(Collections.singletonList(hint));
        String expectedShardedSQL =
            "SELECT id FROM tb_shipping_order_3 USE INDEX (created_at) WHERE created_at < '2017-10-01' AND created_at >= '2015-03-01' AND tb_shipping_order_3.user_id = 721864719 AND 'sharding_index' = 'sharding_index' LIMIT 10";
        assertEquals(selectShardingSQL.results.next().shardedSQL, expectedShardedSQL);
    }

    @Test public void testRewriteIndexHintsInvalidForJoinTableSQL() {
        String sql = "select a.id, b.name from a, b where a.id = 1 limit 1";
        ShardingSQL selectSQL = handleSQL(sql, mixShardingRouter);
        MySqlUseIndexHint hint = new MySqlUseIndexHint();
        hint.setIndexList(Collections.singletonList(new SQLIdentifierExpr("created_at")));
        selectSQL.rewriteSQLHints(Collections.singletonList(hint));
        String expectedSQL = "select a.id, b.name from a, b where a.id = 1 limit 1";
        assertEquals(selectSQL.originSQL, expectedSQL);
    }

    @Test public void testRewriteIndexHintsForNonShardingSingleTableSQL() {
        String sql = "select a.id from a where a.name = 'test' order by created_at";
        ShardingSQL selectSQL = handleSQL(sql, mixShardingRouter);
        MySqlUseIndexHint hint = new MySqlUseIndexHint();
        hint.setIndexList(Collections.singletonList(new SQLIdentifierExpr("created_at")));
        selectSQL.rewriteSQLHints(Collections.singletonList(hint));
        String expectedSQL =
            "SELECT a.id FROM a USE INDEX (created_at) WHERE a.name = 'test' ORDER BY created_at";
        assertEquals(selectSQL.originSQL, expectedSQL);
        assertTrue(selectSQL.sqlFeature.hasOrderBy());
    }

    @Test(expectedExceptions = QueryException.class, expectedExceptionsMessageRegExp = ".*update sql in transaction of batch operate.*")
    public void NotSupportUpdateSQLInBatchMode() {
        String sql = "update tb_shipping_order set a = 1 where tracking_id = 123";
        String batchComment = "/* E:" + Constants.ELE_META_BATCH_ANALYZED_MARKER + "=true:E */";
        Send2BatchCond batchCond =
            new Send2BatchCond().setBatchSendComment(batchComment).setNeedBatchAnalyze(true);
        ShardingSQL shardingSQL = ShardingSQL
            .handleMySQLBasicly(sql, batchComment, mixShardingRouter, batchCond, whiteFieldsFilter);
        ShardingSQL.handleSQLSharding(shardingSQL);
    }

    @Test(expectedExceptions = QueryException.class, expectedExceptionsMessageRegExp = ".*this sql need to be batch sharded but cannot extract composed key.*")
    public void BatchDeleteNoComposeKey() {
        DBChannelDispatcher.getHolders().get(APOLLO_GROUP).getDalGroup().setBatchAllowed(true);
        String sql = "delete tb_shipping_order where retailer_id in (123, 456, 789)";
        Send2BatchCond batchCond =
            new Send2BatchCond().setDalGroupNameAndInitBatchAllowed(APOLLO_GROUP)
                .setNeedBatchAnalyze(true);
        ShardingSQL shardingSQL = ShardingSQL
            .handleMySQLBasicly(sql, "", mixShardingRouter, batchCond, whiteFieldsFilter);
        ShardingSQL.handleSQLSharding(shardingSQL);
        DBChannelDispatcher.getHolders().get(APOLLO_GROUP).getDalGroup().setBatchAllowed(false);
    }

    @Test public void deleteShardingAfterBatchAnalyze() {
        DBChannelDispatcher.getHolders().get(APOLLO_GROUP).getDalGroup().setBatchAllowed(true);
        String sql = "delete tb_shipping_order where tracking_id in (123, 456, 789, 123456)";
        String batchComment = "/* E:" + Constants.ELE_META_BATCH_ANALYZED_MARKER + "=true&"
            + Constants.ELE_META_SHARD_COL_VAL + "=123456:E */";
        Send2BatchCond batchCond =
            new Send2BatchCond().setDalGroupNameAndInitBatchAllowed(APOLLO_GROUP)
                .setBatchSendComment(batchComment);
        ShardingSQL shardingSQL = ShardingSQL
            .handleMySQLBasicly(sql, batchComment, mixShardingRouter, batchCond, whiteFieldsFilter);
        ShardingSQL.handleSQLSharding(shardingSQL);
        List<ShardingResult> actualShardingResults = new ArrayList<>();
        while (shardingSQL.results.hasNext()) {
            actualShardingResults.add(shardingSQL.results.next());
        }
        List<ShardingResult> expectedShardingResults = new ArrayList<>();
        ShardingResult expect = new ShardingResult(
            "/* E:batch=true&shardColVal=123456:E */DELETE FROM tb_shipping_order_288 WHERE tracking_id IN (123, 456, 789, 123456)",
            "tb_shipping_order_288", "apollo_group3");
        expect.id = "576";
        expect.shardingRuleIndex = 0;
        expect.shardingRuleCount = 1;
        expect.queryType = QUERY_TYPE.DELETE;
        expectedShardingResults.add(expect);
        assertEquals(actualShardingResults, expectedShardingResults);
        DBChannelDispatcher.getHolders().get(APOLLO_GROUP).getDalGroup().setBatchAllowed(false);
    }

    @Test public void newOrderShardingUpdateByMappingKey() {
        String sql =
            "update eleme_order set x='1',y=2,z=HOUR('10:05:03') where ORDER_ID = 1234567 and user_id = 123";
        ShardingSQL updateShardingSQL = handleSQL(sql, newOrderShardingRouter);
        List<ShardingResult> actualMappingShardingResult = new ArrayList<>(2);
        while (updateShardingSQL.mappingResults.hasNext()) {
            actualMappingShardingResult.add(updateShardingSQL.mappingResults.next());
        }
        List<ShardingResult> expectedMappingShardingResult = new ArrayList<>(2);
        ShardingResult expectedUserMapping = new ShardingResult(
            "SELECT user_id FROM dal_mapping_order_id_user_id_4 WHERE order_id = '1234567'",
            "dal_mapping_order_id_user_id_4", "new_eos_eleme-zeus_eos_group2");
        expectedUserMapping.shardingRuleIndex = 0;
        expectedUserMapping.shardingRuleCount = 2;
        expectedUserMapping.queryType = QUERY_TYPE.SELECT;
        ShardingResult expectedRstMapping = new ShardingResult(
            "SELECT restaurant_id FROM dal_mapping_order_id_restaurant_id_4 WHERE order_id = '1234567'",
            "dal_mapping_order_id_restaurant_id_4", "new_eos_eleme-zeus_eos_group4");
        expectedRstMapping.shardingRuleIndex = 1;
        expectedRstMapping.shardingRuleCount = 2;
        expectedRstMapping.queryType = QUERY_TYPE.SELECT;
        expectedMappingShardingResult.add(expectedUserMapping);
        expectedMappingShardingResult.add(expectedRstMapping);
        assertEquals(actualMappingShardingResult, expectedMappingShardingResult);

        // query value from mapping
        updateShardingSQL.collectShardingColumnValue(0, "123");
        updateShardingSQL.collectShardingColumnValue(1, "456");
        updateShardingSQL.generateOriginalShardedSQLs();
        List<ShardingResult> actualShardingResults = new ArrayList<>(2);
        while (updateShardingSQL.results.hasNext()) {
            actualShardingResults.add(updateShardingSQL.results.next());
        }
        List<ShardingResult> expectedShardingResults = new ArrayList<>(2);
        ShardingResult expectedUserResult = new ShardingResult(
            "UPDATE eleme_order_uid_4 SET x = '1', y = 2, z = HOUR('10:05:03') WHERE ORDER_ID = 1234567 AND user_id = 123",
            "eleme_order_uid_4", "new_eos_eleme-zeus_eos_group2");
        expectedUserResult.shardingRuleIndex = 0;
        expectedUserResult.shardingRuleCount = 2;
        expectedUserResult.queryType = QUERY_TYPE.UPDATE;
        expectedUserResult.id = "562" + ShardingConfig.SHARDING_ID_DELIMITER + "469";
        ShardingResult expectedRstResult = new ShardingResult(
            "UPDATE eleme_order_rid_3 SET x = '1', y = 2, z = HOUR('10:05:03') WHERE ORDER_ID = 1234567 AND user_id = 123",
            "eleme_order_rid_3", "new_eos_eleme-zeus_eos_group3");
        expectedRstResult.shardingRuleIndex = 1;
        expectedRstResult.shardingRuleCount = 2;
        expectedRstResult.id = "562" + ShardingConfig.SHARDING_ID_DELIMITER + "469";
        expectedRstResult.queryType = QUERY_TYPE.UPDATE;
        expectedShardingResults.add(expectedUserResult);
        expectedShardingResults.add(expectedRstResult);
        assertEquals(actualShardingResults, expectedShardingResults);
    }

    @Test public void newOrderShardingInsertMultiValuesSameShardingId() {
        String sql =
            "insert into eleme_order(order_id, USER_ID, restaurant_id) values(166080, 123, 456), (166112, 123, 456)";
        ShardingSQL insertShardingSQL = handleSQL(sql, newOrderShardingRouter, false);
        List<ShardingResult> actualMappingShardingResult = new ArrayList<>(4);
        while (insertShardingSQL.mappingResults.hasNext()) {
            actualMappingShardingResult.add(insertShardingSQL.mappingResults.next());
        }
        List<ShardingResult> expectedMappingShardingResult = new ArrayList<>(4);
        ShardingResult expectedUserMapping1 = new ShardingResult(
            "INSERT INTO dal_mapping_order_id_user_id_5 (order_id, user_id) VALUES ('166080', '123') ON DUPLICATE KEY UPDATE user_id = '123'",
            "dal_mapping_order_id_user_id_5", "new_eos_eleme-zeus_eos_group2");
        expectedUserMapping1.shardingRuleIndex = 0;
        expectedUserMapping1.shardingRuleCount = 2;
        expectedUserMapping1.queryType = QUERY_TYPE.INSERT;
        ShardingResult expectedRstMapping1 = new ShardingResult(
            "INSERT INTO dal_mapping_order_id_restaurant_id_5 (order_id, restaurant_id) VALUES ('166080', '456') ON DUPLICATE KEY UPDATE restaurant_id = '456'",
            "dal_mapping_order_id_restaurant_id_5", "new_eos_eleme-zeus_eos_group4");
        expectedRstMapping1.shardingRuleIndex = 1;
        expectedRstMapping1.shardingRuleCount = 2;
        expectedRstMapping1.queryType = QUERY_TYPE.INSERT;
        ShardingResult expectedUserMapping2 = new ShardingResult(
            "INSERT INTO dal_mapping_order_id_user_id_3 (order_id, user_id) VALUES ('166112', '123') ON DUPLICATE KEY UPDATE user_id = '123'",
            "dal_mapping_order_id_user_id_3", "new_eos_eleme-zeus_eos_group1");
        expectedUserMapping2.shardingRuleIndex = 0;
        expectedUserMapping2.shardingRuleCount = 2;
        expectedUserMapping2.queryType = QUERY_TYPE.INSERT;
        ShardingResult expectedRstMapping2 = new ShardingResult(
            "INSERT INTO dal_mapping_order_id_restaurant_id_3 (order_id, restaurant_id) VALUES ('166112', '456') ON DUPLICATE KEY UPDATE restaurant_id = '456'",
            "dal_mapping_order_id_restaurant_id_3", "new_eos_eleme-zeus_eos_group3");
        expectedRstMapping2.shardingRuleIndex = 1;
        expectedRstMapping2.shardingRuleCount = 2;
        expectedRstMapping2.queryType = QUERY_TYPE.INSERT;
        expectedMappingShardingResult.add(expectedUserMapping1);
        expectedMappingShardingResult.add(expectedRstMapping1);
        expectedMappingShardingResult.add(expectedUserMapping2);
        expectedMappingShardingResult.add(expectedRstMapping2);
        assertEquals(actualMappingShardingResult, expectedMappingShardingResult);

        insertShardingSQL.generateOriginalShardedSQLs();
        List<ShardingResult> actualShardingResults = new ArrayList<>(2);
        while (insertShardingSQL.results.hasNext()) {
            actualShardingResults.add(insertShardingSQL.results.next());
        }
        List<ShardingResult> expectedShardingResults = new ArrayList<>(2);
        ShardingResult expectedUserResult = new ShardingResult(
            "INSERT INTO eleme_order_uid_4 (order_id, USER_ID, restaurant_id) VALUES (166080, 123, 456), (166112, 123, 456)",
            "eleme_order_uid_4", "new_eos_eleme-zeus_eos_group2");
        expectedUserResult.shardingRuleIndex = 0;
        expectedUserResult.shardingRuleCount = 2;
        expectedUserResult.queryType = QUERY_TYPE.INSERT;
        expectedUserResult.id = "562" + ShardingConfig.SHARDING_ID_DELIMITER + "469";
        ShardingResult expectedRstResult = new ShardingResult(
            "INSERT INTO eleme_order_rid_3 (order_id, USER_ID, restaurant_id) VALUES (166080, 123, 456), (166112, 123, 456)",
            "eleme_order_rid_3", "new_eos_eleme-zeus_eos_group3");
        expectedRstResult.shardingRuleIndex = 1;
        expectedRstResult.shardingRuleCount = 2;
        expectedRstResult.id = "562" + ShardingConfig.SHARDING_ID_DELIMITER + "469";
        expectedRstResult.queryType = QUERY_TYPE.INSERT;
        expectedShardingResults.add(expectedUserResult);
        expectedShardingResults.add(expectedRstResult);
        assertEquals(actualShardingResults, expectedShardingResults);
    }

    @Test(expectedExceptions = QueryException.class, expectedExceptionsMessageRegExp = ".*insert multi rows with different sharding id is not allowed.*")
    public void newOrderShardingInsertMultiValuesDiffShardingId() {
        String sql =
            "insert into eleme_order(order_id, user_id, restaurant_id) values(166080, 123, 456), (111111, 1234, 456)";
        handleSQL(sql, newOrderShardingRouter, false);
    }

    @Test public void queryInsufficientMappingValueUpdateByMappingKey() {
        String sql =
            "update eleme_order set x='1',y=2,z=HOUR('10:05:03') where order_id = 1234567 and user_id = 123";
        ShardingSQL updateShardingSQL = handleSQL(sql, newOrderShardingRouter);
        // query value from mapping
        updateShardingSQL.collectShardingColumnValue(1, "456");
        updateShardingSQL.generateOriginalShardedSQLs();
        List<ShardingResult> actualShardingResults = new ArrayList<>(2);
        while (updateShardingSQL.results.hasNext()) {
            actualShardingResults.add(updateShardingSQL.results.next());
        }
        List<ShardingResult> expectedShardingResults = new ArrayList<>(2);
        ShardingResult expectedUserResult = new ShardingResult(
            "UPDATE eleme_order_uid_7 SET x = '1', y = 2, z = HOUR('10:05:03') WHERE order_id = 1234567 AND user_id = 123",
            "eleme_order_uid_7", "new_eos_eleme-zeus_eos_group2");
        expectedUserResult.shardingRuleIndex = 0;
        expectedUserResult.shardingRuleCount = 2;
        expectedUserResult.queryType = QUERY_TYPE.UPDATE;
        expectedUserResult.id = "last:last";
        ShardingResult expectedRstResult = new ShardingResult(
            "UPDATE eleme_order_rid_7 SET x = '1', y = 2, z = HOUR('10:05:03') WHERE order_id = 1234567 AND user_id = 123",
            "eleme_order_rid_7", "new_eos_eleme-zeus_eos_group4");
        expectedRstResult.shardingRuleIndex = 1;
        expectedRstResult.shardingRuleCount = 2;
        expectedRstResult.queryType = QUERY_TYPE.UPDATE;
        expectedRstResult.id = "last:last";
        expectedShardingResults.add(expectedUserResult);
        expectedShardingResults.add(expectedRstResult);
        assertEquals(actualShardingResults, expectedShardingResults);
    }

    @Test(expectedExceptions = QueryException.class, expectedExceptionsMessageRegExp = ".*this sql need to be sharded by shardingKey but \\(\\[restaurant_id\\]\\) miss some shardingKey then expect shardingKey \\(\\[user_id, restaurant_id\\]\\).*")
    public void insertByShardingKeyMissShardingKey() {
        String sql =
            "insert into eleme_order(order_id, restaurant_id) values(166080, 456), (166112, 456)";
        handleSQL(sql, newOrderShardingRouter, false);
    }

    @Test public void insertMissingMappingKey() {
        String sql = "insert into eleme_order(user_id, restaurant_id) values(123, 456), (123, 456)";
        ShardingSQL updateShardingSQL = handleSQL(sql, newOrderShardingRouter, false);
        List<ShardingResult> actualShardingResults = new ArrayList<>(2);
        while (updateShardingSQL.results.hasNext()) {
            actualShardingResults.add(updateShardingSQL.results.next());
        }
        List<ShardingResult> expectedShardingResults = new ArrayList<>(2);
        ShardingResult expectedUserResult = new ShardingResult(
            "INSERT INTO eleme_order_uid_4 (user_id, restaurant_id) VALUES (123, 456), (123, 456)",
            "eleme_order_uid_4", "new_eos_eleme-zeus_eos_group2");
        expectedUserResult.shardingRuleIndex = 0;
        expectedUserResult.shardingRuleCount = 2;
        expectedUserResult.queryType = QUERY_TYPE.INSERT;
        expectedUserResult.id = "562" + ShardingConfig.SHARDING_ID_DELIMITER + "469";
        ShardingResult expectedRstResult = new ShardingResult(
            "INSERT INTO eleme_order_rid_3 (user_id, restaurant_id) VALUES (123, 456), (123, 456)",
            "eleme_order_rid_3", "new_eos_eleme-zeus_eos_group3");
        expectedRstResult.shardingRuleIndex = 1;
        expectedRstResult.shardingRuleCount = 2;
        expectedRstResult.id = "562" + ShardingConfig.SHARDING_ID_DELIMITER + "469";
        expectedRstResult.queryType = QUERY_TYPE.INSERT;
        expectedShardingResults.add(expectedUserResult);
        expectedShardingResults.add(expectedRstResult);
        assertEquals(actualShardingResults, expectedShardingResults);
    }

    @Test(expectedExceptions = QueryException.class, expectedExceptionsMessageRegExp = ".*this sql need to be sharded but \\[user_id\\] is not mapping key.*")
    public void updateOnlyOneDimShardingKeyNewOrderSharding() {
        String sql = "update eleme_order set description = 'test' where USer_id = 123";
        handleSQL(sql, newOrderShardingRouter, false);
    }

    @Test(description = "测试非sharding表进入batch delete逻辑的情况")
    public void nonShardingTableBatchDelete() {
        DBChannelDispatcher.getHolders().get(APOLLO_GROUP).getDalGroup().setBatchAllowed(true);
        String sql = "delete from non_sharding_table where id in (1,2,3,4,5,6,7,8,9)";
        try {
            Send2BatchCond batchCond =
                new Send2BatchCond().setDalGroupNameAndInitBatchAllowed(APOLLO_GROUP)
                    .setNeedBatchAnalyze(true);
            ShardingSQL shardingSQL = ShardingSQL
                .handleMySQLBasicly(sql, "", mixShardingRouter, batchCond, whiteFieldsFilter);
            ShardingSQL.handleSQLSharding(shardingSQL);
        } catch (Exception e) {
            fail("expect success");
        } finally {
            DBChannelDispatcher.getHolders().get(APOLLO_GROUP).getDalGroup().setBatchAllowed(false);
        }
    }

    @Test public void doubleQuotedExprPattern() throws Exception {
        String sql =
            "SELECT id, phone, settled_at, created_at, updated_at FROM eleme_order WHERE created_at >= \"2018-01-04 14:17:07\" AND created_at <= \"2018-01-04 14:18:07\"";
        ShardingSQL shardingSQL = handleSQL(sql, mixShardingRouter, false);
        assertEquals(shardingSQL.getOriginMostSafeSQL(),
            "SELECT id, phone, settled_at, created_at, updated_at FROM eleme_order WHERE created_at >= ? AND created_at <= ?");
    }

    @Test public void deleteNoWhere() {
        String sql = "delete from abc";
        ShardingSQL shardingSQL = handleSQL(sql, ShardingRouter.defaultShardingRouter, false);
        assertFalse(shardingSQL.sqlFeature.hasWhere());
    }

    @Test public void testSqlCut() {
        String sql = "SELECT id FROM eleme_order ";
        ShardingSQL shardingSQL = handleSQL(sql, mixShardingRouter, false);

        int maxLength = GreySwitch.getInstance().getMaxSqlPatternTruncLength();
        StringBuilder stringBuilder = new StringBuilder(maxLength);
        IntStream.range(0, maxLength).forEach(i -> stringBuilder.append("i"));
        assertEquals(stringBuilder.toString().length(), maxLength);
        shardingSQL.setMostSafeSQL(stringBuilder.toString());
        assertEquals(shardingSQL.getMostSafeSQL().length(), maxLength);

        stringBuilder.setLength(0);
        IntStream.range(0, maxLength + 100).forEach(i -> stringBuilder.append("i"));
        shardingSQL.setMostSafeSQL(stringBuilder.toString());
        assertEquals(shardingSQL.getMostSafeSQL().length(), maxLength);

        stringBuilder.setLength(0);
        IntStream.range(0, maxLength - 1).forEach(i -> stringBuilder.append("i"));
        shardingSQL.setMostSafeSQL(stringBuilder.toString());
        assertEquals(stringBuilder.toString().length(), maxLength - 1);
        assertEquals(shardingSQL.getMostSafeSQL().length(), maxLength - 1);
    }

    @Test public void unionQuery() {
        String sql =
            "select t1.id as name1 from eleme_order t1 where t1.name1 in ('100', 101, 102, 103, 104, 200) and id2 =205 and id3 in (1,2)"
                + " union "
                + "select t1.id as name1 from eleme_order t1 where t1.name1 in ('100', 101, 102, 103, 104, 200) and id2 =206 and id3 in (3,4)";
        ShardingSQL shardingSQL = handleSQL(sql, mixShardingRouter);
        assertFalse(shardingSQL.needSharding);
    }
}
