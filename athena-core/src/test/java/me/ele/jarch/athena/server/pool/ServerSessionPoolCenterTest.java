package me.ele.jarch.athena.server.pool;

import me.ele.jarch.athena.allinone.DBConnectionInfo;
import me.ele.jarch.athena.allinone.DBVendor;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * Created by jinghao.wang on 16/9/12.
 */
public class ServerSessionPoolCenterTest {
    DBConnectionInfo master0;
    DBConnectionInfo master1;
    DBConnectionInfo master2;
    DBConnectionInfo master3;
    DBConnectionInfo master4;
    DBConnectionInfo slave1;
    DBConnectionInfo slave2;
    DBConnectionInfo slave3;
    DBConnectionInfo slave4;
    DBConnectionInfo slave5;
    DBConnectionInfo fake;

    @BeforeTest public void setUp() throws Exception {
        master0 = DBConnectionInfo.parseDBConnectionString(
            "eosgroup   daltestdb        127.0.0.1 3306 master master0 root 3JC9GhGkX25z9S2fRk6UYw==");
        master1 = DBConnectionInfo.parseDBConnectionString(
            "eusgroup   daltestdb        127.0.0.1 3306 master master0 root 3JC9GhGkX25z9S2fRk6UYw==");
        master2 = DBConnectionInfo.parseDBConnectionString(
            "ersgroup   daltestdb        127.0.0.1 3307 master master0 root 3JC9GhGkX25z9S2fRk6UYw==");
        master3 = DBConnectionInfo.parseDBConnectionString(
            "ersgroup   daltestdb        127.0.0.1 3307 master master0 root 3JC9GhGkX25z9S2fRk6UYw== allow_autocommit_switch=true&@@SQL_SELECT_LIMIT=100");
        master4 = DBConnectionInfo.parseDBConnectionString(
            "eusgroup   daltestdb        127.0.0.1 3307 master master0 root 3JC9GhGkX25z9S2fRk6UYw== allow_autocommit_switch=true&@@SQL_SELECT_LIMIT=100");
        slave1 = DBConnectionInfo.parseDBConnectionString(
            "eosgroup   daltestdb        127.0.0.1 3307 slave  slave1  root 3JC9GhGkX25z9S2fRk6UYw==");
        slave2 = DBConnectionInfo.parseDBConnectionString(
            "eosgroup   daltestdb        127.0.0.1 3306 slave  slave2  root 3JC9GhGkX25z9S2fRk6UYw==");
        slave3 = DBConnectionInfo.parseDBConnectionString(
            "eosgroup   daltestdb        127.0.0.1 3306 slave  slave3  root 3JC9GhGkX25z9S2fRk6UYw==");
        slave4 = DBConnectionInfo.parseDBConnectionString(
            "ersgroup   daltestdb        127.0.0.1 3306 slave  slave3  root 3JC9GhGkX25z9S2fRk6UYw== allow_autocommit_switch=true&@@SQL_SELECT_LIMIT=100");
        slave5 = DBConnectionInfo.parseDBConnectionString(
            "eusgroup   daltestdb        127.0.0.1 3306 slave  slave4  root 3JC9GhGkX25z9S2fRk6UYw== allow_autocommit_switch=true&@@SQL_SELECT_LIMIT=100");
        fake = DBConnectionInfo.parseDBConnectionString(
            "fakegroup  fakedb           0.0.0.0   3306 dummy  dummy   fake 3JC9GhGkX25z9S2fRk6UYw==");
    }

    @AfterMethod public void tearDown() throws Exception {
        ServerSessionPoolCenter.getEntities().clear();
    }

    @Test public void testGetServerSessionPool() throws Exception {
        ServerSessionPool master0NonAutoCommit =
            ServerSessionPoolCenter.getInst().getServerSessionPool(master0, false);
        ServerSessionPool master0AutoCommit =
            ServerSessionPoolCenter.getInst().getServerSessionPool(master0, true);
        ServerSessionPool master1NonAutoCommit =
            ServerSessionPoolCenter.getInst().getServerSessionPool(master1, false);
        ServerSessionPool master1AutoCommit =
            ServerSessionPoolCenter.getInst().getServerSessionPool(master1, true);
        ServerSessionPool master2NonAutoCommit =
            ServerSessionPoolCenter.getInst().getServerSessionPool(master2, false);
        ServerSessionPool master2AutoCommit =
            ServerSessionPoolCenter.getInst().getServerSessionPool(master2, true);
        //测试ServerSessionPoolManager对master的缓存作用
        Assert.assertSame(master0NonAutoCommit, master1NonAutoCommit);
        Assert.assertSame(master0AutoCommit, master1AutoCommit);
        Assert.assertNotSame(master0NonAutoCommit, master0AutoCommit);
        Assert.assertNotSame(master1NonAutoCommit, master1AutoCommit);
        Assert.assertNotSame(master2NonAutoCommit, master0NonAutoCommit);
        Assert.assertNotSame(master2AutoCommit, master1AutoCommit);

        ServerSessionPool slave1 =
            ServerSessionPoolCenter.getInst().getServerSessionPool(this.slave1, true);
        ServerSessionPool slave2 =
            ServerSessionPoolCenter.getInst().getServerSessionPool(this.slave2, true);
        ServerSessionPool slave3 =
            ServerSessionPoolCenter.getInst().getServerSessionPool(this.slave3, true);
        //测试ServerSessionPoolManager对slave的缓存作用
        Assert.assertSame(slave3, slave2);
        Assert.assertNotSame(slave1, slave2);
        Assert.assertNotSame(slave1, slave3);
    }

    @Test public void testGetSickServerSessionPool() throws Exception {
        ServerSessionPool fake1 =
            ServerSessionPoolCenter.getInst().getSickServerSessionPool(DBVendor.MYSQL);
        ServerSessionPool fake2 =
            ServerSessionPoolCenter.getInst().getSickServerSessionPool(DBVendor.MYSQL);
        Assert.assertSame(fake1, fake2);
    }

    @Test public void testCloseServerSessionPool() throws Exception {
        ServerSessionPool master0NonAutoCommit =
            ServerSessionPoolCenter.getInst().getServerSessionPool(master0, false);
        ServerSessionPool master1NonAutoCommit =
            ServerSessionPoolCenter.getInst().getServerSessionPool(master1, false);
        ServerSessionPool master2NonAutoCommit =
            ServerSessionPoolCenter.getInst().getServerSessionPool(master2, false);

        Assert.assertEquals(master0NonAutoCommit.refCnt(), 2);
        Assert.assertEquals(master2NonAutoCommit.refCnt(), 1);
        master1NonAutoCommit.release();
        master0NonAutoCommit.release();
        Assert.assertNull(ServerSessionPoolCenter.getEntities().get(new PoolId(master0, false)));
        Assert.assertNotNull(ServerSessionPoolCenter.getEntities().get(new PoolId(master2, false)));
    }

    @Test public void testAdditionalCfgSessionPool() throws Exception {
        ServerSessionPool master2AutoCommit =
            ServerSessionPoolCenter.getInst().getServerSessionPool(master2, true);
        ServerSessionPool master3AutoCommit =
            ServerSessionPoolCenter.getInst().getServerSessionPool(master3, true);
        ServerSessionPool master4AutoCommit =
            ServerSessionPoolCenter.getInst().getServerSessionPool(master4, true);
        Assert.assertSame(master3AutoCommit, master4AutoCommit);
        Assert.assertNotSame(master2AutoCommit, master4AutoCommit);
        ServerSessionPool slave3 =
            ServerSessionPoolCenter.getInst().getServerSessionPool(this.slave3, true);
        ServerSessionPool slave4 =
            ServerSessionPoolCenter.getInst().getServerSessionPool(this.slave4, true);
        ServerSessionPool slave5 =
            ServerSessionPoolCenter.getInst().getServerSessionPool(this.slave5, true);
        Assert.assertNotSame(slave3, slave4);
        Assert.assertSame(slave4, slave5);
    }
}
