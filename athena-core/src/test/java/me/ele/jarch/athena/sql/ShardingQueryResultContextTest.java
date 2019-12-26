package me.ele.jarch.athena.sql;

import org.testng.annotations.Test;

/**
 * Created by jinghao.wang on 2017/8/3.
 */
public class ShardingQueryResultContextTest {
    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Called .* in ShardingQueryResultContext")
    public void testAddCommandComplete() throws Exception {
        ShardingQueryResultContext sqrc = new ShardingQueryResultContext();
        sqrc.addCommandComplete(new byte[0]);
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Called .* in ShardingQueryResultContext")
    public void testAddReadyForQuery() throws Exception {
        ShardingQueryResultContext sqrc = new ShardingQueryResultContext();
        sqrc.addCommandComplete(new byte[0]);
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Called .* in ShardingQueryResultContext")
    public void testSendAndFlush2Client() throws Exception {
        ShardingQueryResultContext sqrc = new ShardingQueryResultContext();
        sqrc.addCommandComplete(new byte[0]);
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Called .* in ShardingQueryResultContext")
    public void testAddRowDescription() throws Exception {
        ShardingQueryResultContext sqrc = new ShardingQueryResultContext();
        sqrc.addRowDescription(new byte[0]);
    }
}