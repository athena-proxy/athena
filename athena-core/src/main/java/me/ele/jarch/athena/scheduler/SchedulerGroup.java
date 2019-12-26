package me.ele.jarch.athena.scheduler;

import me.ele.jarch.athena.allinone.DBConnectionInfo;
import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.util.ZKCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SchedulerGroup {
    private static final Logger logger = LoggerFactory.getLogger(SchedulerGroup.class);
    private final String channelName;
    private volatile Map<String, Scheduler> schedulers = new HashMap<String, Scheduler>();
    private final ZKCache zkCache;

    public SchedulerGroup(String channelName, ZKCache zkCache) {
        this.zkCache = zkCache;
        this.channelName = channelName;
    }

    synchronized public Scheduler getScheduler(String id) throws QuitException {
        Scheduler scheduler = schedulers.get(id);
        if (scheduler != null) {
            return scheduler;
        } else {
            throw new QuitException(
                "DBGroup:" + this.channelName + " cannot get Scheduler for the id " + id);
        }
    }

    synchronized public void addSchedulers(List<DBConnectionInfo> infos) {
        Map<String, Scheduler> newschedulers = new HashMap<String, Scheduler>(this.schedulers);
        for (DBConnectionInfo info : infos) {
            Scheduler newScheduler = new Scheduler(info, zkCache, channelName);
            newScheduler.start();
            Scheduler oldScheduler = newschedulers.put(info.getQualifiedDbId(), newScheduler);
            if (oldScheduler != null) {
                oldScheduler.close();
                logger.error("Cover older Scheduler : " + info.getQualifiedDbId());
            }
        }
        this.schedulers = newschedulers;
    }

    synchronized public void removeSchedulers(List<DBConnectionInfo> infos) {
        Map<String, Scheduler> newschedulers = new HashMap<String, Scheduler>(this.schedulers);
        for (DBConnectionInfo info : infos) {
            Scheduler oldScheduler = newschedulers.remove(info.getQualifiedDbId());
            if (oldScheduler != null) {
                oldScheduler.close();
            } else {
                logger.error("Try to remove a nonexistent Scheduler : " + info.getQualifiedDbId());
            }
        }
        this.schedulers = newschedulers;
    }

    List<Scheduler> getSchedulers() {
        List<Scheduler> schedulerList = new ArrayList<Scheduler>();
        this.schedulers.forEach((id, s) -> {
            schedulerList.add(s);
        });
        return schedulerList;
    }
}
