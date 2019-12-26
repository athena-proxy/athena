package me.ele.jarch.athena.scheduler;

import me.ele.jarch.athena.util.ZKCache;
import me.ele.jarch.athena.util.deploy.DALGroup;

/**
 * Created by jinghao.wang on 16/11/25.
 */
public class MockDBChannelDispatcher extends DBChannelDispatcher {
    public MockDBChannelDispatcher(DALGroup dalGroup, ZKCache zkCache) {
        super(dalGroup, zkCache);
    }

    @Override public boolean isDalGroupInitialized() {
        return true;
    }
}
