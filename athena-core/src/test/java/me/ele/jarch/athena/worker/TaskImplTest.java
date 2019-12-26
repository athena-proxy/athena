package me.ele.jarch.athena.worker;

import me.ele.jarch.athena.netty.SqlSessionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.stream.IntStream;

public class TaskImplTest extends Task {
    private static final Logger logger = LoggerFactory.getLogger(TaskImplTest.class);

    private boolean isSleep = false;

    public TaskImplTest(String name, SqlSessionContext ctx, boolean isSleep) {
        super(name, ctx);
        this.isSleep = isSleep;
    }

    public TaskImplTest(String name, SqlSessionContext ctx, TaskPriority priority) {
        super(name, ctx, priority);
    }

    @Override public void execute() {
        if (isSleep) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                logger.error("Catch InterruptedException:", e);
                // e.printStackTrace();
            }
        } else {
            // System.out.println(toString());
        }
    }

    public static void main(String[] args) throws InterruptedException {
        SqlSessionContext ctx = new SqlSessionContext(null, "1.1.1.1");
        TaskImplTest[] taskImplTests = new TaskImplTest[100];
        TaskImplTest testSleep = new TaskImplTest("sleep", ctx, true);

        for (int i = 0; i < 100; i++) {
            if (i < 30) {
                taskImplTests[i] = new TaskImplTest("NORMAL" + i, ctx, TaskPriority.NORMAL);
            } else if (i >= 30 && i < 70) {
                taskImplTests[i] = new TaskImplTest("ABOVE" + i, ctx, TaskPriority.ABOVE);
            } else {
                taskImplTests[i] = new TaskImplTest("HIGH" + i, ctx, TaskPriority.HIGH);
            }
        }

        SchedulerWorker.getInstance().enqueue(testSleep);

        Integer[] arrays = getRandomNum(100, 100);
        for (int i = 0; i < arrays.length; i++) {
            SchedulerWorker.getInstance().enqueue(taskImplTests[arrays[i]]);
        }

        BlockingQueue<Runnable>[] queues = SchedulerWorker.getInstance().getQueues();
        IntStream.range(0, queues.length).forEach(i -> {
            queues[i].forEach(a -> System.out.println("worker-" + i + "|" + a.toString()));
        });

        System.out.println("complete!");

    }

    public static Integer[] getRandomNum(int num, int n) {
        Set<Integer> sets = new HashSet<Integer>();
        Random random = new Random();
        while (sets.size() < n) {
            sets.add(random.nextInt(num));
        }
        return sets.toArray(new Integer[n]);
    }
}
