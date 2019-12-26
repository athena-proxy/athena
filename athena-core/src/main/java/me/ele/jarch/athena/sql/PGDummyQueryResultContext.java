package me.ele.jarch.athena.sql;

/**
 * Created by jinghao.wang on 17/6/21.
 */
public class PGDummyQueryResultContext extends PGQueryResultContext {

    public PGDummyQueryResultContext() {
    }

    @Override protected AbstractIntColumnWatcher createIntColumnWatcher() {
        return EmptyIntColumnWatcher.EMPTY;
    }

    @Override public void writeBytes(byte[] src) {
        count += src.length;
        sqlCtx.currentWriteToDalCounts = count;
    }

    @Override public void sendAndFlush2Client() {
        throw new RuntimeException("Called sendAndFlush2Client in PGDummyQueryResultContext");
    }
}
