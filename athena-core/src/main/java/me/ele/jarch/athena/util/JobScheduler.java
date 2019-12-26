package me.ele.jarch.athena.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.DelayQueue;

/**
 * Created by donxuu on 8/3/15.
 */
public class JobScheduler extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(JobScheduler.class);
    private int runningRounds = 0;
    private final String birthday = new SimpleDateFormat().format(new Date());

    private DelayQueue<Job> delayQueue = new DelayQueue<>();

    public JobScheduler(String jobSchedulerName) {
        this.setName(jobSchedulerName);
        this.addJob(this.getName() + "_statistics", 5 * 60 * 1000, () -> {
            if (logger.isDebugEnabled()) {
                logger.debug(String
                    .format("%d jobs were executed in %s since %s", JobScheduler.this.runningRounds,
                        JobScheduler.this.dumpAllJobs(), JobScheduler.this.birthday));
            }
        });
    }

    public Job addJob(String name, long intervalInMilli, JobMethod jobMethod) {
        logger.info(
            String.format("Job : %s, is registered in JobScheduler: %s", name, this.getName()));
        return addJob(name, intervalInMilli, jobMethod, false);
    }

    /**
     * 添加之后不会马上执行的job
     */
    public Job addFirstDelayJob(String name, long intervalInMilli, JobMethod jobMethod) {
        logger.info(
            String.format("Job : %s, is registered in JobScheduler: %s", name, this.getName()));
        return addJob(name, intervalInMilli, jobMethod, false, true);
    }

    public Job addJob(String name, long intervalInMilli, JobMethod jobMethod,
        boolean isOnetimeJob) {
        return addJob(name, intervalInMilli, jobMethod, isOnetimeJob, false);
    }

    public Job addJob(String name, long intervalInMilli, JobMethod jobMethod, boolean isOnetimeJob,
        boolean isFirstDelay) {
        Job job = new Job(this, name, intervalInMilli, jobMethod);
        if (isOnetimeJob) {
            job.setOneTimeJob(true);
            job.setNextTimestampInMilli();
        }
        if (isFirstDelay) {
            job.setNextTimestampInMilli();
        }
        this.delayQueue.offer(job);
        return job;
    }

    public Job addOneTimeJob(String name, long intervalInMilli, JobMethod jobMethod) {
        return addJob(name, intervalInMilli, jobMethod, true);
    }

    @Override public void run() {
        while (true) {
            NoThrow.call(() -> {
                Job job = null;
                boolean runAgain = true;
                try {
                    job = delayQueue.take();
                    if (job.isCanceled()) {
                        runAgain = false;
                        logger.info(job.name + " is cancelled");
                    } else {
                        job.execute();
                    }
                    runAgain = job.isOneTimeJob() ? false : runAgain;
                } catch (Exception t) {
                    logger.error(String
                        .format("%s Unexpected Exception when running jobs. Ignore this time. ",
                            this.getName()), t);
                } finally {
                    if (runAgain && job != null) {
                        job.setNextTimestampInMilli();
                        delayQueue.offer(job);
                    }
                }
            });
        }
    }

    /**
     * @return Return a string contains all job names in this {@code JobScheduler}.
     * 注意，此方法主要作代码调试用途。当job数量较多时，此方法的效率很低，不应在使用频繁的正常路径调用此方法。
     */
    private String dumpAllJobs() {
        final StringBuilder rel = new StringBuilder(4096);
        rel.append("[[Job scheduler ").append(this.getName()).append("] ");
        this.delayQueue.forEach(i -> rel.append(", ").append(i.name));
        rel.append("]");
        return rel.toString();
    }

    public void increaseCount() {
        this.runningRounds++;
    }
}
