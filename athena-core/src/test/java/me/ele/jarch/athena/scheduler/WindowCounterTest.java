package me.ele.jarch.athena.scheduler;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Created by jinghao.wang on 16/4/7.
 */
public class WindowCounterTest {

    @Test public void testGetLastIndex() throws Exception {
        WindowCounter counter = new WindowCounter(5);
        counter.record(1);
        counter.record(1);
        counter.record(1);
        Assert.assertEquals(3, counter.getLastIndex());
        counter.record(1);
        counter.record(1);
        counter.record(1);
        Assert.assertEquals(6, counter.getLastIndex());
    }

    @Test public void testGetHistory() throws Exception {
        WindowCounter counter = new WindowCounter(5);
        counter.record(1);
        counter.record(2);
        counter.record(3);
        Assert.assertEquals(6, counter.getHistory());
        counter.record(4);
        counter.record(5);
        Assert.assertEquals(15, counter.getHistory());
        counter.record(6);
        Assert.assertEquals(20, counter.getHistory());
        counter.record(7);
        Assert.assertEquals(25, counter.getHistory());
        counter.record(8);
        Assert.assertEquals(30, counter.getHistory());
    }

    @Test public void testAllMatch() throws Exception {
        WindowCounter counter = new WindowCounter(5);
        counter.record(1);
        counter.record(2);
        counter.record(3);
        Assert.assertFalse(counter.allMatch(i -> i > 0));
        counter.record(4);
        counter.record(5);
        Assert.assertTrue(counter.allMatch(i -> i > 0));
        Assert.assertFalse(counter.allMatch(i -> i > 1));

    }
}
