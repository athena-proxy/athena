package me.ele.jarch.athena.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * Created by donxuu on 8/4/15.
 */
public class Job implements Delayed {
    private static final Logger logger = LoggerFactory.getLogger(Job.class);
    public final JobMethod jobMethod;
    private final JobScheduler jobScheduler;
    private volatile long intervalInMilli;
    public final String name;
    protected long nextTimestampInMilli = System.currentTimeMillis();
    private volatile boolean canceled = false;
    private volatile boolean isOneTimeJob = false;

    public Job(JobScheduler jobScheduler, String name, long intervalInMilli, JobMethod jobMethod) {
        this.jobScheduler = jobScheduler;
        this.name = name;
        this.jobMethod = jobMethod;
        this.intervalInMilli = intervalInMilli;
    }

    public void execute() {
        NoThrow.call(() -> {
            try {
                if (logger.isDebugEnabled()) {
                    logger.debug(String
                        .format("Running jobMethod : %s, in JobScheduler : %s", this.name,
                            jobScheduler.getName()));
                }

                jobMethod.invoke();
                jobScheduler.increaseCount();
            } catch (Exception t) {
                logger.error(String
                    .format("Failed to execute jobMethod: %s in JobScheduler: %s.", name,
                        jobScheduler.getName()), t);
            }
        });
    }

    public void cancel() {
        this.canceled = true;

    }

    public boolean isCanceled() {
        return this.canceled;
    }

    public void setNextTimestampInMilli() {
        this.nextTimestampInMilli = System.currentTimeMillis() + intervalInMilli;
    }

    @Override public long getDelay(TimeUnit unit) {
        return unit
            .convert(nextTimestampInMilli - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    @Override public int compareTo(Delayed o) {
        return Long
            .compare(this.getDelay(TimeUnit.MILLISECONDS), o.getDelay(TimeUnit.MILLISECONDS));
    }

    public boolean isOneTimeJob() {
        return isOneTimeJob;
    }

    protected void setOneTimeJob(boolean isOneTimeJob) {
        this.isOneTimeJob = isOneTimeJob;
    }

    public long getIntervalInMilli() {
        return intervalInMilli;
    }

    public void setIntervalInMilli(long intervalInMilli) {
        this.intervalInMilli = intervalInMilli;
    }
}

