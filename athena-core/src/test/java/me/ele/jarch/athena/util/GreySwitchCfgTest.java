package me.ele.jarch.athena.util;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;


public class GreySwitchCfgTest {

    @Test public void testGreySwitchCfg() throws InterruptedException {

        GreySwitchCfg
            .tryLoadCfg(getClass().getClassLoader().getResource("dal-grey-switch.cfg").getFile());
        assertFalse(GreySwitch.getInstance().isSmartAutoKillerOpen());
        assertFalse(GreySwitch.getInstance().isAllowSendAuditSqlToRmq());
        assertEquals(GreySwitch.getInstance().getLoginSlowInMills(), -1);
        assertEquals(GreySwitch.getInstance().getLoginTimeoutInMills(), -1);
        assertEquals(GreySwitch.getInstance().getOverflowTraceFrequency(), -1);
        assertFalse(GreySwitch.getInstance().isAllowDalGroupHealthCheck());
    }
}
