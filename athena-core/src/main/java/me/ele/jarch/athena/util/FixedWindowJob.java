package me.ele.jarch.athena.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zhengchao on 16/7/7.
 */
public class FixedWindowJob {
    private Logger logger = LoggerFactory.getLogger(FixedWindowJob.class);
    private int window;
    private JobScheduler jobScheduler;
    private JobMethod jobMethod;
    private volatile boolean stops = false;

    public FixedWindowJob(JobScheduler jobScheduler, int window) {
        this.window = window;
        this.jobScheduler = jobScheduler;
    }

    public void start(JobMethod jobMethod) {
        if (jobMethod == null) {
            return;
        }
        this.jobMethod = jobMethod;

        addOneTimeJob("FixedWindowJob", this::internalJob);
    }

    public void stop() {
        stops = true;
    }

    public void setWindowSize(int newWindow) {
        if (newWindow <= 0 || newWindow > Integer.MAX_VALUE)
            return;
        this.window = newWindow;
    }

    private void internalJob() {
        if (stops || this.jobMethod == null) {
            return;
        }

        try {
            this.jobMethod.invoke();
        } catch (Exception e) {
            logger.error("oops!", e);
        }

        addOneTimeJob("FixWindowJob", this::internalJob);
    }

    private Job addOneTimeJob(String name, JobMethod jobMethod) {
        long currTimes = System.currentTimeMillis();
        long nextWindowEnd = (currTimes / window + 1) * window;
        return jobScheduler
            .addOneTimeJob(name, nextWindowEnd - System.currentTimeMillis(), jobMethod);
    }
}
