package me.ele.jarch.athena.worker;

import me.ele.jarch.athena.netty.AthenaServer;
import me.ele.jarch.athena.netty.Monitorable;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.util.AthenaConfig;
import me.ele.jarch.athena.util.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.IntStream;

public class SchedulerWorker {
    private static final Logger logger = LoggerFactory.getLogger(SchedulerWorker.class);
    private static final int threadPoolSize = AthenaConfig.getInstance().getWorkingThreadsCount();
    private static final SchedulerWorker INSTANCE = new SchedulerWorker();

    ThreadPoolExecutor[] executors;
    @SuppressWarnings("unchecked") private final BlockingQueue<Runnable>[] queues =
        new PriorityBlockingQueue[threadPoolSize];
    private final List<Job> jobs = new ArrayList<>();

    public SchedulerWorker() {
        executors = new ThreadPoolExecutor[threadPoolSize];
        IntStream.range(0, threadPoolSize).forEach(i -> {
            queues[i] = new PriorityBlockingQueue<>();
            Monitorable workerMonitorJob = new SchedulerWorkerMonitorJob(queues[i]);
            jobs.add(AthenaServer.commonJobScheduler
                .addJob("monitor_schduler_worker-" + i + "_queue_size", 2 * 1000,
                    workerMonitorJob::monitor));
            executors[i] = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, queues[i],
                new ThreadFactory() {
                    @Override public Thread newThread(Runnable r) {
                        Thread t = new Thread(r);
                        t.setName("worker-" + i);
                        return t;
                    }
                });
        });
    }

    public static SchedulerWorker getInstance() {
        return INSTANCE;
    }

    public static int getPoolSize() {
        return threadPoolSize;
    }

    public BlockingQueue<Runnable>[] getQueues() {
        return queues;
    }

    public boolean enqueue(String sessionTaskName, SqlSessionContext ctx) {
        return enqueue(new SessionTask(sessionTaskName, ctx, TaskPriority.NORMAL));
    }

    public boolean enqueue(Task task) {
        executors[task.getIndex()].execute(task);
        return true;
    }

    public void shutdown() {
        IntStream.range(0, threadPoolSize).forEach(i -> {
            executors[i].shutdown();
            try {
                executors[i].awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.error("InterruptedException in shutdown()", e);
                // e.printStackTrace();
            }
        });
        jobs.forEach(job -> job.cancel());
    }
}
