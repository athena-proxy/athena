package me.ele.jarch.athena.sql;

import org.testng.annotations.Test;

/**
 * Created by jinghao.wang on 2017/8/3.
 */
public class PGDummyQueryResultContextTest {
    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Called .* in PGDummyQueryResultContext")
    public void testSendAndFlush2Client() throws Exception {
        PGDummyQueryResultContext dummyQueryResultContext = new PGDummyQueryResultContext();
        dummyQueryResultContext.sendAndFlush2Client();
    }
}
