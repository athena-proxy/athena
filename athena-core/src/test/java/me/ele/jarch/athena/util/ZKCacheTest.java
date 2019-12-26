package me.ele.jarch.athena.util;

import com.alibaba.druid.sql.ast.SQLHint;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlForceIndexHint;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlIgnoreIndexHint;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlIndexHint;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlUseIndexHint;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.scheduler.DBChannelDispatcher;
import me.ele.jarch.athena.util.deploy.DALGroup;
import org.testng.annotations.Test;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;

import static org.testng.Assert.*;

/**
 * Created by jinghao.wang on 16/2/17.
 */
public class ZKCacheTest {

    @Test public void testSetZkCfgNormal() throws Exception {
        ZKCache zkCache = new ZKCache("abc_test_group");
        zkCache.setZkCfg(Constants.AK_UPPER_SIZE, "99");
        assertEquals(99, zkCache.getUpperAutoKillerSize());
        zkCache.setZkCfg(Constants.AK_UPPER_SIZE, "");
        assertEquals(320, zkCache.getUpperAutoKillerSize());
    }

    @Test public void testOfflinechange() throws Exception {
        ZKCache zkCache = new ZKCache("abc_test_group");
        DBChannelDispatcher testDBChannelDispatcher =
            new DBChannelDispatcher(new DALGroup("abc_test_group", "test", "groups"), zkCache);
        DBChannelDispatcher.getHolders().put("abc_test_group", testDBChannelDispatcher);
        zkCache.setZkCfg(Constants.SLAVE_OFFLINE_FLAGS, "groups:slave1;");
        assertTrue(zkCache.getOfflineDbs().contains("groups:slave1"));

        zkCache.setZkCfg(Constants.SLAVE_OFFLINE_FLAGS, "groups:slave2;");
        assertFalse(zkCache.getOfflineDbs().contains("groups:slave1"));
        assertTrue(zkCache.getOfflineDbs().contains("groups:slave2"));

        zkCache.setZkCfg(Constants.SLAVE_OFFLINE_FLAGS, "");
        assertFalse(zkCache.getOfflineDbs().contains("groups:slave1"));
        assertFalse(zkCache.getOfflineDbs().contains("groups:slave2"));
    }

    @Test public void isBindMaster() throws Exception {
        ZKCache zkCache = new ZKCache("abc_test_group");
        SimpleDateFormat parserSDF = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        Calendar calendar = Calendar.getInstance();
        assertFalse(zkCache.isInBindMasterPeriod());

        calendar.add(Calendar.SECOND, 3);
        String end = parserSDF.format(calendar.getTime());
        zkCache.setZkCfg(Constants.BIND_MASTER_PERIOD, end);
        assertTrue(zkCache.isInBindMasterPeriod());

        zkCache.setZkCfg(Constants.BIND_MASTER_PERIOD, "xxx");
        assertFalse(zkCache.isInBindMasterPeriod());

        zkCache.setZkCfg(Constants.BIND_MASTER_PERIOD, "");
        assertFalse(zkCache.isInBindMasterPeriod());

        calendar.add(Calendar.SECOND, -10);
        end = parserSDF.format(calendar.getTime());
        zkCache.setZkCfg(Constants.BIND_MASTER_PERIOD, end);
        assertFalse(zkCache.isInBindMasterPeriod());
    }

    @Test public void whiteFieldsDeadLineEffictive() throws Exception {
        ZKCache zkCache = new ZKCache("abc_test_group");
        SimpleDateFormat parserSDF = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

        zkCache.setZkCfg(Constants.WHITE_FIELDS, "a");

        assertFalse(zkCache.getZKWhiteFieldsFilter().test("a"));

        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.SECOND, 3);
        String end = parserSDF.format(calendar.getTime());
        zkCache.setZkCfg(Constants.WHITE_FIELDS_DEADLINE, end);
        assertTrue(zkCache.getZKWhiteFieldsFilter().test("a"));

        calendar.add(Calendar.SECOND, -10);
        end = parserSDF.format(calendar.getTime());
        zkCache.setZkCfg(Constants.WHITE_FIELDS_DEADLINE, end);
        assertFalse(zkCache.getZKWhiteFieldsFilter().test("a"));

        zkCache.setZkCfg(Constants.WHITE_FIELDS_DEADLINE, "239u2ey8r");
        assertFalse(zkCache.getZKWhiteFieldsFilter().test("a"));
    }

    @Test public void whiteFieldsFormat() throws Exception {
        ZKCache zkCache = new ZKCache("abc_test_group");
        SimpleDateFormat parserSDF = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

        zkCache.setZkCfg(Constants.WHITE_FIELDS, "a");

        assertFalse(zkCache.getZKWhiteFieldsFilter().test("a"));

        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.SECOND, 10);
        String end = parserSDF.format(calendar.getTime());
        zkCache.setZkCfg(Constants.WHITE_FIELDS_DEADLINE, end);

        assertTrue(zkCache.getZKWhiteFieldsFilter().test("a"));

        zkCache.setZkCfg(Constants.WHITE_FIELDS, " a, b");
        assertTrue(zkCache.getZKWhiteFieldsFilter().test("a"));
        assertTrue(zkCache.getZKWhiteFieldsFilter().test("b"));

        zkCache.setZkCfg(Constants.WHITE_FIELDS, "a,b,c");
        assertTrue(zkCache.getZKWhiteFieldsFilter().test("a"));
        assertTrue(zkCache.getZKWhiteFieldsFilter().test("b"));
        assertTrue(zkCache.getZKWhiteFieldsFilter().test("c"));

        zkCache.setZkCfg(Constants.WHITE_FIELDS, "");
        assertFalse(zkCache.getZKWhiteFieldsFilter().test("b"));

        zkCache.setZkCfg(Constants.WHITE_FIELDS, null);
        assertFalse(zkCache.getZKWhiteFieldsFilter().test("c"));
    }

    @Test public void manualBindMasterSQL() throws Exception {
        ZKCache zkCache = new ZKCache("abc_test_group");
        zkCache.setZkCfg(Constants.BIND_MASTER_SQLS, "1b209c03fd540fdea7f29018c1db1d10");
        assertTrue(zkCache.isBindMasterSQL("1b209c03fd540fdea7f29018c1db1d10"));
        assertFalse(zkCache.isBindMasterSQL("2805522dd41e1b57da11967ac5fa258c"));

        zkCache.setZkCfg(Constants.BIND_MASTER_SQLS,
            "1b209c03fd540fdea7f29018c1db1d10;f8756e03c316c1e3e52545c037d2323c");
        assertTrue(zkCache.isBindMasterSQL("1b209c03fd540fdea7f29018c1db1d10"));
        assertTrue(zkCache.isBindMasterSQL("f8756e03c316c1e3e52545c037d2323c"));
        assertFalse(zkCache.isBindMasterSQL("2805522dd41e1b57da11967ac5fa258c"));

        zkCache.setZkCfg(Constants.BIND_MASTER_SQLS,
            "1b209c03fd540fdea7f29018c1db1d10; ;f8756e03c316c1e3e52545c037d2323c");
        assertTrue(zkCache.isBindMasterSQL("1b209c03fd540fdea7f29018c1db1d10"));
        assertTrue(zkCache.isBindMasterSQL("f8756e03c316c1e3e52545c037d2323c"));
        assertFalse(zkCache.isBindMasterSQL("2805522dd41e1b57da11967ac5fa258c"));
        assertFalse(zkCache.isBindMasterSQL(" "));

        zkCache.setZkCfg(Constants.BIND_MASTER_SQLS, "");
        assertFalse(zkCache.isBindMasterSQL("1b209c03fd540fdea7f29018c1db1d10"));
        assertFalse(zkCache.isBindMasterSQL("f8756e03c316c1e3e52545c037d2323c"));
        assertFalse(zkCache.isBindMasterSQL("2805522dd41e1b57da11967ac5fa258c"));
        assertFalse(zkCache.isBindMasterSQL(""));
    }

    @Test public void testUseIndexHints() throws Exception {
        ZKCache zkCache = new ZKCache("abc_test_group");
        zkCache.setZkCfg(Constants.APPEND_INDEX_HINTS,
            "b167eab8da9f5084ee7b0de4e0dfac58:use index(created_at)");
        assertTrue(zkCache.isAppendIndexHintsSQL("b167eab8da9f5084ee7b0de4e0dfac58"));
        assertFalse(zkCache.isAppendIndexHintsSQL("77a13e6c52b67aa044b9e2172a75233c"));
        List<SQLHint> hints =
            zkCache.getAppendIndexHintsBySQLHash("b167eab8da9f5084ee7b0de4e0dfac58").hints;
        assertEquals(hints.size(), 1);
        assertTrue(hints.get(0) instanceof MySqlUseIndexHint);
    }

    @Test public void testIgnoreIndexHints() throws Exception {
        ZKCache zkCache = new ZKCache("abc_test_group");
        zkCache.setZkCfg(Constants.APPEND_INDEX_HINTS,
            "b167eab8da9f5084ee7b0de4e0dfac58:ignore index(created_at)");
        assertTrue(zkCache.isAppendIndexHintsSQL("b167eab8da9f5084ee7b0de4e0dfac58"));
        assertFalse(zkCache.isAppendIndexHintsSQL("77a13e6c52b67aa044b9e2172a75233c"));
        List<SQLHint> hints =
            zkCache.getAppendIndexHintsBySQLHash("b167eab8da9f5084ee7b0de4e0dfac58").hints;
        assertEquals(hints.size(), 1);
        assertTrue(hints.get(0) instanceof MySqlIgnoreIndexHint);
    }

    @Test public void testForceIndexHints() throws Exception {
        ZKCache zkCache = new ZKCache("abc_test_group");
        zkCache.setZkCfg(Constants.APPEND_INDEX_HINTS,
            "b167eab8da9f5084ee7b0de4e0dfac58:FORCE index(created_at)");
        assertTrue(zkCache.isAppendIndexHintsSQL("b167eab8da9f5084ee7b0de4e0dfac58"));
        assertFalse(zkCache.isAppendIndexHintsSQL("77a13e6c52b67aa044b9e2172a75233c"));
        List<SQLHint> hints =
            zkCache.getAppendIndexHintsBySQLHash("b167eab8da9f5084ee7b0de4e0dfac58").hints;
        assertEquals(hints.size(), 1);
        assertTrue(hints.get(0) instanceof MySqlForceIndexHint);
    }

    @Test public void testInvalidIndexHints() throws Exception {
        ZKCache zkCache = new ZKCache("abc_test_group");
        zkCache.setZkCfg(Constants.APPEND_INDEX_HINTS,
            "b167eab8da9f5084ee7b0de4e0dfac58:invalid index(created_at) hint");
        assertFalse(zkCache.isAppendIndexHintsSQL("b167eab8da9f5084ee7b0de4e0dfac58"));
        List<SQLHint> hints =
            zkCache.getAppendIndexHintsBySQLHash("b167eab8da9f5084ee7b0de4e0dfac58").hints;
        assertEquals(hints.size(), 0);
        zkCache.setZkCfg(Constants.APPEND_INDEX_HINTS, "dhfkshfowirehewuih");
        assertFalse(zkCache.isAppendIndexHintsSQL("b167eab8da9f5084ee7b0de4e0dfac58"));
        assertEquals(
            zkCache.getAppendIndexHintsBySQLHash("b167eab8da9f5084ee7b0de4e0dfac58").hints.size(),
            0);
    }

    @Test public void testMultiHintsForOneSQL() throws Exception {
        ZKCache zkCache = new ZKCache("abc_test_group");
        zkCache.setZkCfg(Constants.APPEND_INDEX_HINTS,
            "b167eab8da9f5084ee7b0de4e0dfac58:USE INDEX (i1) IGNORE INDEX FOR ORDER BY (i2)");
        assertTrue(zkCache.isAppendIndexHintsSQL("b167eab8da9f5084ee7b0de4e0dfac58"));
        assertFalse(zkCache.isAppendIndexHintsSQL("77a13e6c52b67aa044b9e2172a75233c"));
        List<SQLHint> hints =
            zkCache.getAppendIndexHintsBySQLHash("b167eab8da9f5084ee7b0de4e0dfac58").hints;
        assertEquals(hints.size(), 2);
        assertTrue(hints.get(0) instanceof MySqlUseIndexHint);
        assertTrue(hints.get(1) instanceof MySqlIgnoreIndexHint);
        assertEquals(MySqlIndexHint.Option.ORDER_BY,
            ((MySqlIgnoreIndexHint) hints.get(1)).getOption());
    }

    @Test public void testMultiIndexForOneSQL() throws Exception {
        ZKCache zkCache = new ZKCache("abc_test_group");
        zkCache.setZkCfg(Constants.APPEND_INDEX_HINTS,
            "b167eab8da9f5084ee7b0de4e0dfac58:USE INDEX (col1_index,col2_index)");
        assertTrue(zkCache.isAppendIndexHintsSQL("b167eab8da9f5084ee7b0de4e0dfac58"));
        assertFalse(zkCache.isAppendIndexHintsSQL("77a13e6c52b67aa044b9e2172a75233c"));
        List<SQLHint> hints =
            zkCache.getAppendIndexHintsBySQLHash("b167eab8da9f5084ee7b0de4e0dfac58").hints;
        assertEquals(hints.size(), 1);
        assertTrue(hints.get(0) instanceof MySqlUseIndexHint);
        assertEquals(((MySqlUseIndexHint) hints.get(0)).getIndexList().size(), 2);
    }

    @Test public void testMultiSQLIndexHints() throws Exception {
        ZKCache zkCache = new ZKCache("abc_test_group");
        zkCache.setZkCfg(Constants.APPEND_INDEX_HINTS,
            "b167eab8da9f5084ee7b0de4e0dfac58:USE INDEX (i1) IGNORE INDEX FOR ORDER BY (i2);77a13e6c52b67aa044b9e2172a75233c:FORCE index(created_at)");
        assertTrue(zkCache.isAppendIndexHintsSQL("b167eab8da9f5084ee7b0de4e0dfac58"));
        assertTrue(zkCache.isAppendIndexHintsSQL("77a13e6c52b67aa044b9e2172a75233c"));
        List<SQLHint> hints =
            zkCache.getAppendIndexHintsBySQLHash("b167eab8da9f5084ee7b0de4e0dfac58").hints;
        assertEquals(hints.size(), 2);
        assertTrue(hints.get(0) instanceof MySqlUseIndexHint);
        assertTrue(hints.get(1) instanceof MySqlIgnoreIndexHint);
        assertEquals(MySqlIndexHint.Option.ORDER_BY,
            ((MySqlIgnoreIndexHint) hints.get(1)).getOption());
        List<SQLHint> secondHints =
            zkCache.getAppendIndexHintsBySQLHash("77a13e6c52b67aa044b9e2172a75233c").hints;
        assertEquals(secondHints.size(), 1);
        assertTrue(secondHints.get(0) instanceof MySqlForceIndexHint);
    }
}
