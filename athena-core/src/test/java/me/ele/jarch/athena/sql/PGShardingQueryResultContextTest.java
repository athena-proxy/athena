package me.ele.jarch.athena.sql;

import org.testng.annotations.Test;

/**
 * Created by jinghao.wang on 2017/8/3.
 */
public class PGShardingQueryResultContextTest {
    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Called .* in PGShardingQueryResultContext")
    public void testSendAndFlush2Client() throws Exception {
        PGShardingQueryResultContext sqrc = new PGShardingQueryResultContext();
        sqrc.sendAndFlush2Client();
    }

}