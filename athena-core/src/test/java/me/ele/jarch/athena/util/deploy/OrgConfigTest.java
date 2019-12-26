package me.ele.jarch.athena.util.deploy;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

public class OrgConfigTest {

    @BeforeClass public void beforeTest() {
        // for test
        OrgConfig.getInstance().loadConfig(
            getClass().getClassLoader().getResource("conf/goproxy-front-port.cfg.template")
                .getFile());
    }

    @Test public void getDalGroupsByOrg() {
        Assert.assertEquals(OrgConfig.getInstance().getDalGroupsByOrg("exs").size(), 0);
        List<DALGroup> eosGroups = OrgConfig.getInstance().getDalGroupsByOrg("eos");
        Assert.assertEquals(eosGroups.size(), 2);
        Assert
            .assertTrue(eosGroups.stream().anyMatch(g -> g.getDbGroup().equals("eos_main_group")));
        Assert.assertTrue(eosGroups.stream()
            .anyMatch(g -> g.getName().equals("eos_main_group__sub__bpm_talos_search")));

        List<DALGroup> naposGroups = OrgConfig.getInstance().getDalGroupsByOrg("napos");
        Assert.assertEquals(naposGroups.size(), 10);
        Assert.assertTrue(naposGroups.stream().allMatch(g -> g.getDbGroup().equals("napos_group")));
        Assert.assertTrue(naposGroups.stream()
            .anyMatch(g -> g.getName().equals("napos-payservice_srv_wager_group")));
        // for test
        OrgConfig.getInstance().loadConfig("conf/goproxy-front-port.cfg");
    }
}
