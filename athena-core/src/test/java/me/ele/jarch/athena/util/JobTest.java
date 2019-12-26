package me.ele.jarch.athena.util;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Created by donxuu on 8/3/15.
 */
public class JobTest {
    @Test public void testAddJob() throws InterruptedException {
        JobScheduler jobScheduler = new JobScheduler("test");
        jobScheduler.start();
        final int[] job1_flag = {0};
        final int[] job2_flag = {0};
        Job job1 = jobScheduler.addJob("abc", 50, () -> job1_flag[0] += 1);
        Job job2 = jobScheduler.addJob("def", 50, () -> job2_flag[0] += 1);

        // 普通Job,addJob后即使interval没到,也会先执行一次
        Thread.sleep(10);
        Assert.assertEquals(job1_flag[0], 1);
        Assert.assertEquals(job2_flag[0], 1);

        Thread.sleep(20);
        Assert.assertEquals(job1_flag[0], 1);
        Assert.assertEquals(job2_flag[0], 1);

        Thread.sleep(30);
        Assert.assertEquals(job1_flag[0], 2);
        Assert.assertEquals(job2_flag[0], 2);

        job1.cancel();
        job2.cancel();
        Thread.sleep(60);
        Assert.assertEquals(job1_flag[0], 2);
        Assert.assertEquals(job2_flag[0], 2);

    }

    @Test public void testAddFirstDelayJob() throws InterruptedException {
        JobScheduler jobScheduler = new JobScheduler("testAddFirstDelayJob");
        jobScheduler.start();
        // addJob后如果interval没到,不会执行
        final int[] job3_flag_testFirstDelayJob = {0};
        Job job3 =
            jobScheduler.addFirstDelayJob("def", 50, () -> job3_flag_testFirstDelayJob[0] += 1);
        Thread.sleep(30);
        Assert.assertEquals(job3_flag_testFirstDelayJob[0], 0);
        Thread.sleep(30);
        Assert.assertEquals(job3_flag_testFirstDelayJob[0], 1);
        Thread.sleep(60);
        Assert.assertEquals(job3_flag_testFirstDelayJob[0], 2);
        job3.cancel();
        Thread.sleep(60);
        Assert.assertEquals(job3_flag_testFirstDelayJob[0], 2);
    }
}
