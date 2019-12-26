package me.ele.jarch.athena.allinone;

import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.scheduler.SchedulerGroup;
import me.ele.jarch.athena.server.pool.ServerSessionPool;
import me.ele.jarch.athena.util.AthenaConfig;
import me.ele.jarch.athena.util.GrayType;
import me.ele.jarch.athena.util.ZKCache;
import me.ele.jarch.athena.util.deploy.DALGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class DBGroupTest {
    private static final Logger logger = LoggerFactory.getLogger(DBGroupTest.class);
    int preINIT_SERVERSESSION_NUM;

    @BeforeMethod public void before() {
        preINIT_SERVERSESSION_NUM = ServerSessionPool.getInitServerSessionNum();
        ServerSessionPool.setInitServerSessionNum(0);
    }

    @AfterMethod public void after() {
        ServerSessionPool.setInitServerSessionNum(preINIT_SERVERSESSION_NUM);
    }

    @Test(expectedExceptions = QuitException.class, expectedExceptionsMessageRegExp = ".* has not been initialized")
    public void testDBGroupNotInit() throws QuitException {
        ZKCache zkCache = new ZKCache("testDalGroup");
        DBGroup dbGroup = new DBGroup("test", zkCache, new SchedulerGroup("testDalGroup", zkCache),
            new DALGroup("testDalGroup", "", "test"));
        dbGroup.getDBServer(DBRole.MASTER);
    }

    @Test(expectedExceptions = QuitException.class, expectedExceptionsMessageRegExp = ".* no available master pool")
    public void InitDBGroup() throws QuitException {
        AthenaConfig.getInstance().setGrayType(GrayType.NOSHARDING);
        DBConnectionInfo info1 =
            new DBConnectionInfo("test", "daltestdb", "192.168.80.19", 3306, "root", "root",
                DBRole.MASTER, "0", true);
        info1.setReadOnly(false);
        List<DBConnectionInfo> infos = new ArrayList<DBConnectionInfo>();
        infos.add(info1);
        MockSchedulerGroup schedulerGroup = new MockSchedulerGroup("test");
        MockScheduler sch1 = new MockScheduler(info1);
        schedulerGroup.putScheduler(sch1);
        ZKCache zkCache = new ZKCache("testDalGroup");
        DBGroup dbGroup =
            new DBGroup("test", zkCache, schedulerGroup, new DALGroup("testDalGroup", "", "test"));
        dbGroup.init();
        dbGroup.getDbGroupUpdater().updateByDBCfgs(infos);
        Assert.assertEquals(dbGroup.getDBServer(DBRole.MASTER), info1.getQualifiedDbId());
        Assert.assertEquals(dbGroup.getDBServer(DBRole.MASTER), dbGroup.getDBServer(DBRole.SLAVE));
        //clear heartbeat results
        dbGroup.getDbGroupUpdater().updateByDBCfgs(Collections.emptyList());

        DBConnectionInfo info2 =
            new DBConnectionInfo("test", "daltestdb", "192.168.80.19", 3306, "root", "root",
                DBRole.SLAVE, "1", true);
        infos.add(info2);
        MockScheduler sch2 = new MockScheduler(info2);
        schedulerGroup.putScheduler(sch2);
        dbGroup.getDbGroupUpdater().updateByDBCfgs(infos);
        Assert.assertEquals(dbGroup.getDBServer(DBRole.MASTER), info1.getQualifiedDbId());
        Assert.assertEquals(dbGroup.getDBServer(DBRole.SLAVE), info2.getQualifiedDbId());
        infos.clear();
        dbGroup.getDbGroupUpdater().updateByDBCfgs(infos);
        dbGroup.getDBServer(DBRole.MASTER);
    }


    @Test public void LoadBalance() throws InterruptedException, QuitException {
        DBConnectionInfo info1 =
            new DBConnectionInfo("test", "daltestdb", "192.168.80.139", 3306, "root", "root",
                DBRole.SLAVE, "0", true);
        DBConnectionInfo info2 =
            new DBConnectionInfo("test", "daltestdb", "192.168.80.139", 3306, "root", "root",
                DBRole.SLAVE, "1", true);
        DBConnectionInfo info3 =
            new DBConnectionInfo("test", "daltestdb", "192.168.80.139", 3306, "root", "root",
                DBRole.SLAVE, "2", true);
        DBConnectionInfo info4 =
            new DBConnectionInfo("test", "daltestdb", "192.168.80.139", 3306, "root", "root",
                DBRole.SLAVE, "3", true);
        List<DBConnectionInfo> infos = new ArrayList<DBConnectionInfo>();
        infos.add(info1);
        infos.add(info2);
        infos.add(info3);
        infos.add(info4);

        MockSchedulerGroup schedulerGroup = new MockSchedulerGroup("test");
        MockScheduler sch1 = new MockScheduler(info1);
        MockScheduler sch2 = new MockScheduler(info2);
        MockScheduler sch3 = new MockScheduler(info3);
        MockScheduler sch4 = new MockScheduler(info4);
        schedulerGroup.putScheduler(sch1);
        schedulerGroup.putScheduler(sch2);
        schedulerGroup.putScheduler(sch3);
        schedulerGroup.putScheduler(sch4);
        ZKCache zkCache = new ZKCache("testDalGroup");
        DBGroup dbGroup =
            new DBGroup("test", zkCache, schedulerGroup, new DALGroup("testDalGroup", "", "test"));
        dbGroup.init();
        dbGroup.getDbGroupUpdater().updateByDBCfgs(infos);
        sch1.setSize(5);
        sch2.setSize(2);
        sch3.setSize(9);
        sch4.setSize(10);
        Assert.assertEquals(dbGroup.getDBServer(DBRole.SLAVE), info2.getQualifiedDbId());

        sch1.setSize(0);
        sch2.setSize(3);
        sch3.setSize(5000);
        sch4.setSize(111);
        Assert.assertEquals(dbGroup.getDBServer(DBRole.SLAVE), info1.getQualifiedDbId());

        sch1.setSize(4);
        sch2.setSize(3);
        sch3.setSize(0);
        sch4.setSize(111);
        Assert.assertEquals(dbGroup.getDBServer(DBRole.SLAVE), info3.getQualifiedDbId());

        sch1.setSize(10);
        sch2.setSize(10);
        sch3.setSize(5);
        sch4.setSize(0);
        Assert.assertEquals(dbGroup.getDBServer(DBRole.SLAVE), info4.getQualifiedDbId());


        sch1.setSize(10);
        sch2.setSize(10);
        sch3.setSize(10);
        sch4.setSize(10);
        Map<String, AtomicInteger> record = new TreeMap<>();
        record.put(info1.getQualifiedDbId(), new AtomicInteger(0));
        record.put(info2.getQualifiedDbId(), new AtomicInteger(0));
        record.put(info3.getQualifiedDbId(), new AtomicInteger(0));
        record.put(info4.getQualifiedDbId(), new AtomicInteger(0));
        List<Thread> threads = new ArrayList<>();
        for (int threadCount = 0; threadCount < 4; threadCount++) {
            Runnable test = new Runnable() {
                @Override public void run() {
                    for (int i = 0; i < 1000_000; i++) {
                        String id = "";
                        try {
                            id = dbGroup.getDBServer(DBRole.SLAVE);
                        } catch (QuitException e1) {
                            logger.error("Catch QuitException:", e1);
                            // e1.printStackTrace();
                        }
                        record.get(id).incrementAndGet();
                    }
                }
            };
            Thread testThread = new Thread(test);
            threads.add(testThread);
        }

        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        Assert.assertEquals(1000_000, record.get(info1.getQualifiedDbId()).get());
        Assert.assertEquals(1000_000, record.get(info2.getQualifiedDbId()).get());
        Assert.assertEquals(1000_000, record.get(info3.getQualifiedDbId()).get());
        Assert.assertEquals(1000_000, record.get(info4.getQualifiedDbId()).get());
    }

}
