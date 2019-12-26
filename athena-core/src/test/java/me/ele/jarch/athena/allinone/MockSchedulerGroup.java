package me.ele.jarch.athena.allinone;

import me.ele.jarch.athena.scheduler.Scheduler;
import me.ele.jarch.athena.scheduler.SchedulerGroup;
import me.ele.jarch.athena.util.ZKCache;

import java.util.HashMap;
import java.util.Map;

public class MockSchedulerGroup extends SchedulerGroup {

    private Map<String, MockScheduler> schedulers = new HashMap<>();

    public MockSchedulerGroup(String groupName) {
        super(groupName, new ZKCache(groupName));
    }

    public MockSchedulerGroup(String groupName, ZKCache zkCache) {
        super(groupName, zkCache);
    }

    public Scheduler getScheduler(String id) {
        return schedulers.get(id);
    }

    public void putScheduler(MockScheduler sch) {
        schedulers.put(sch.getInfo().getQualifiedDbId(), sch);
    }

}
