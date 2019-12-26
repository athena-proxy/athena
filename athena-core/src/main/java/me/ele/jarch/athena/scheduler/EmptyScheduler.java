package me.ele.jarch.athena.scheduler;

import me.ele.jarch.athena.allinone.DBConnectionInfo;
import me.ele.jarch.athena.util.ZKCache;

public class EmptyScheduler extends Scheduler {
    public static final Scheduler emptySched = new EmptyScheduler();

    private EmptyScheduler() {
        super(null, new ZKCache(""), "");
    }

    @Override public String toString() {
        return "EmptyScheduler []";
    }

    @Override public DBConnectionInfo getInfo() {
        return new DBConnectionInfo();
    }
}
