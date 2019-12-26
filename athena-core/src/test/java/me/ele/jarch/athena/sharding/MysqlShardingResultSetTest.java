package me.ele.jarch.athena.sharding;

import org.testng.annotations.Test;

import java.util.Collections;

/**
 * Created by jinghao.wang on 2017/8/3.
 */
public class MysqlShardingResultSetTest {
    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Called .* in MysqlShardingResultSet")
    public void testAddCommandComplete() throws Exception {
        ShardingResultSet resultSet = new MysqlShardingResultSet(Collections.emptyList(), false);
        resultSet.addCommandComplete(new byte[0]);
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Called .* in MysqlShardingResultSet")
    public void testAddReadyForQuery() throws Exception {
        ShardingResultSet resultSet = new MysqlShardingResultSet(Collections.emptyList(), false);
        resultSet.addReadyForQuery(new byte[0]);
    }

}
