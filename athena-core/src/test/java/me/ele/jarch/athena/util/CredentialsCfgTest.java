package me.ele.jarch.athena.util;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Created by jinghao.wang on 16/6/3.
 */
public class CredentialsCfgTest {

    @BeforeMethod public void setUp() throws Exception {
        CredentialsCfg
            .loadConfig(getClass().getClassLoader().getResource("dal-credentials.cfg").getFile());
    }

    @Test public void testConfig() throws Exception {
        Assert.assertTrue(CredentialsCfg.getConfig().keySet().contains("eosgroup_root"));
        Assert.assertTrue(CredentialsCfg.getConfig().keySet().contains("eos_group@eos_group"));
        Assert.assertTrue(
            CredentialsCfg.getConfig().keySet().contains("eosgroup_readonly_user@eosgroup"));
        Assert.assertTrue(CredentialsCfg.getConfig().keySet().contains("beta_group"));
        Assert.assertTrue(CredentialsCfg.getConfig().keySet().contains("eosgroup"));
        Assert.assertTrue(CredentialsCfg.getConfig().keySet().contains("eusgroup"));
    }

    @Test public void testEncryptedPassword() throws Exception {
        Assert.assertEquals(CredentialsCfg.getConfig().get("beta_group").getEncryptedPwd(),
            "81F5E21E35407D884A6CD4A731AEBFB6AF209E1B");
        Assert.assertNotEquals(CredentialsCfg.getConfig().get("beta_group").getEncryptedPwd(),
            "81F5E21E35407D884A6CD4A731AEBFB6AF209E1B ");
    }

    @Test public void testReadOnly() throws Exception {
        Assert.assertTrue(
            CredentialsCfg.getConfig().get("eosgroup_readonly_user@eosgroup").isReadOnly());
        Assert.assertFalse(CredentialsCfg.getConfig().get("eosgroup").isReadOnly());
    }

    @Test public void testSchema() throws Exception {
        Assert.assertEquals(
            CredentialsCfg.getConfig().get("eosgroup_readonly_user@eosgroup").getSchema(),
            "eosgroup");
        Assert.assertEquals(CredentialsCfg.getConfig().get("beta_group").getSchema(), "");
        Assert.assertNotEquals(CredentialsCfg.getConfig().get("beta_group").getSchema(),
            "beta_group");
    }
}
