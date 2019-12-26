package me.ele.jarch.athena.util;

import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Set;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Created by jinghao.wang on 15/12/16.
 */
public class DBChannelCfgMonitorTest {

    @Test public void testBuildDatabaseConnectString() throws Exception {
        Set<String> testSet = new HashSet<>();
        testSet.add("abc_group");
        testSet.add("group_abc");
        testSet.add("abc_group__sub__");
        testSet.add("__abc_group__");
        String result = DBChannelCfgMonitor.getInstance().buildDatabaseConnectString(testSet);
        assertTrue(result.contains("abc_group"));
        assertTrue(result.contains("group_abc"));
        assertTrue(result.contains("abc_group__sub__"));
        assertTrue(result.contains("__abc_group__"));
        assertFalse(result.contains("__abc_group__1"));
        assertFalse(result.contains("1__abc_group__"));
        assertFalse(result.contains("1__abc_2group__"));
        System.out.println(result);

        // 测试空Set
        String emptryResult =
            DBChannelCfgMonitor.getInstance().buildDatabaseConnectString(new HashSet<>());
        assertTrue(emptryResult.isEmpty());
        Set<String> nullSet = null;
        String nullResult = DBChannelCfgMonitor.getInstance().buildDatabaseConnectString(nullSet);
        assertTrue(nullResult.isEmpty());
    }

    @Test public void testLoadDBConfig() throws Exception {

        Set<String> configSet = DBChannelCfgMonitor.getInstance()
            .getAllCfgs(getClass().getClassLoader().getResource("dal-proxy-ports.cfg").getFile());
        assertTrue(configSet.contains("eosgroup"));
    }
}
