package me.ele.jarch.athena.scheduler;

import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.util.GreySwitch;
import me.ele.jarch.athena.util.ZKCache;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AutoKillerTest {

    @Test public void getSmartAutoKillerSlowSQL() {
        ZKCache zkCache = new ZKCache("test");
        GreySwitch.getInstance().setSmartAutoKillerOpen(false);
        zkCache.setZkCfg(Constants.AK_UPPER_SIZE, "32");
        zkCache.setZkCfg(Constants.AK_LOWER_SIZE, "8");
        zkCache.setZkCfg(Constants.AK_SLOW_SQL, "1000");
        Assert.assertEquals(32, zkCache.getSmartUpperAutoKillerSize());
        Assert.assertEquals(8, zkCache.getSmartLowerAutoKillerSize());
        Assert.assertEquals(1000, zkCache.getSmartAutoKillerSlowSQL(0));
        Assert.assertEquals(1000, zkCache.getSmartAutoKillerSlowSQL(8));
        GreySwitch.getInstance().setSmartAutoKillerOpen(true);
        Assert.assertEquals(0, zkCache.getSmartUpperAutoKillerSize());
        Assert.assertEquals(0, zkCache.getSmartLowerAutoKillerSize());
        Assert.assertEquals(Integer.MAX_VALUE, zkCache.getSmartAutoKillerSlowSQL(0));
        Assert.assertEquals(1000, zkCache.getSmartAutoKillerSlowSQL(32));
        Assert.assertEquals(4000, zkCache.getSmartAutoKillerSlowSQL(8));
        Assert.assertEquals(32000, zkCache.getSmartAutoKillerSlowSQL(1));
    }
}
