package me.ele.jarch.athena.util.deploy.dalgroupcfg;

import java.util.concurrent.atomic.AtomicInteger;

public class AegisRquest {

    public final String dalgroupId;

    public final String dalgroup;

    public final AtomicInteger retryCount;

    public final String aegisUrl;

    public AegisRquest(String dalgroup, Deployments deployments) {
        this.dalgroupId = deployments.getId();
        this.dalgroup = dalgroup;
        this.aegisUrl = deployments.getAegisUrl();
        retryCount = new AtomicInteger(3);
    }

    public long getDelayMilli() {
        if (retryCount.get() == 2) {
            return 2_000;
        }

        if (retryCount.get() == 1) {
            return 5_000;
        }

        if (retryCount.get() == 0) {
            return 10_000;
        }
        return -1;
    }
}
