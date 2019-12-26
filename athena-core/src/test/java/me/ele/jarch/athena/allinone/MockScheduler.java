package me.ele.jarch.athena.allinone;

import me.ele.jarch.athena.scheduler.Scheduler;
import me.ele.jarch.athena.util.ZKCache;

public class MockScheduler extends Scheduler {
    private int size = 0;

    public MockScheduler(DBConnectionInfo info) {
        super(info, new ZKCache(info.getGroup()), "");
    }

    public MockScheduler(DBConnectionInfo info, ZKCache zkCache) {
        super(info, zkCache, "");
    }

    public int getQueueBlockingTaskCount() {
        return size;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }
}
