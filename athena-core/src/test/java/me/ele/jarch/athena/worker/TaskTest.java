package me.ele.jarch.athena.worker;

import me.ele.jarch.athena.netty.SqlSessionContext;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;


public class TaskTest {

    @Test public void test() {
        BlockingQueue<Task> queue = new PriorityBlockingQueue<>();

        TaskImplTest[] taskImplTests = new TaskImplTest[90];
        SqlSessionContext ctx = new SqlSessionContext(null, "1.1.1.1");

        for (int i = 0; i < 90; i++) {
            if (i < 30) {
                taskImplTests[i] = new TaskImplTest("NORMAL" + i, ctx, TaskPriority.NORMAL);
            } else if (i >= 30 && i < 60) {
                taskImplTests[i] = new TaskImplTest("ABOVE" + i, ctx, TaskPriority.ABOVE);
            } else {
                taskImplTests[i] = new TaskImplTest("HIGH" + i, ctx, TaskPriority.HIGH);
            }
        }

        Integer[] arrays = getRandomNum(90, 90);
        for (int i = 0; i < arrays.length; i++) {
            queue.add(taskImplTests[arrays[i]]);
        }
        Task task;
        int i = 0;
        while ((task = queue.poll()) != null) {
            if (i < 30) {
                Assert.assertEquals(task.getName(), "HIGH" + (i + 60));
            } else if (i >= 30 && i < 60) {
                Assert.assertEquals(task.getName(), "ABOVE" + i);
            } else {
                Assert.assertEquals(task.getName(), "NORMAL" + (i - 60));
            }

            i++;
        }
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
