package me.ele.jarch.athena.scheduler;

import me.ele.jarch.athena.allinone.DBConnectionInfo;
import me.ele.jarch.athena.util.ZKCache;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Created by jinghao.wang on 16/1/7.
 */
public class IdDecoratorTest {

    @SuppressWarnings({"rawtypes", "unchecked"}) @Test public void testHashCode() throws Exception {
        Scheduler scheduler =
            new Scheduler(new DBConnectionInfo(), new ZKCache("abc_group"), "abc_group");
        Scheduler scheduler1 =
            new Scheduler(new DBConnectionInfo(), new ZKCache("abc_group_1"), "abc_group_1");
        Assert.assertEquals(new IdDecorator(scheduler).hashCode(),
            new IdDecorator(scheduler).hashCode());
        Assert.assertNotSame(new IdDecorator(scheduler).hashCode(),
            new IdDecorator(scheduler1).hashCode());
    }

}
