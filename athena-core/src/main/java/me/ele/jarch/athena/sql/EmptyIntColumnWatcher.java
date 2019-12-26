package me.ele.jarch.athena.sql;

import me.ele.jarch.athena.netty.SqlSessionContext;

import java.util.List;

/**
 * Created by zhengchao on 2017/2/27.
 */
public class EmptyIntColumnWatcher extends AbstractIntColumnWatcher {
    public static final EmptyIntColumnWatcher EMPTY = new EmptyIntColumnWatcher();

    @Override
    public void startIntColumnAnalyze(final SqlSessionContext sqlCtx, final byte[] cmdColPacket) {
    }

    @Override public void fireTraceIfNecessary(final byte[] packet) {
    }

    @Override protected List<String> extractContent(byte[] packet) {
        return null;
    }

    @Override protected void addWatchingColumnIfNecessary(byte[] cmdColPacket) {
    }
}
