package me.ele.jarch.athena.util;

import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.scheduler.DBChannelDispatcher;
import me.ele.jarch.athena.util.deploy.DALGroup;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public class FastFailFilterTest {

    @Test public void testFastFailFilter() {

        DALGroup eus = new DALGroup("eus", "eus", "eus");
        ZKCache zkCache = new ZKCache("eus");
        DBChannelDispatcher holdereus = new DBChannelDispatcher(eus, zkCache);
        DBChannelDispatcher.getHolders().put("eus", holdereus);

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.SECOND, +3);

        zkCache.setZkCfg(Constants.MASTER_HEARTBEAT_FASTFAIL_PERIOD, format.format(cal.getTime()));

        try {
            Assert.assertTrue(FastFailFilter.checkDBGroupFastFailOpen("eus"));
            Thread.sleep(4000);
            Assert.assertFalse(FastFailFilter.checkDBGroupFastFailOpen("eus"));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
