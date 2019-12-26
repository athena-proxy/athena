package me.ele.jarch.athena.scheduler;

import me.ele.jarch.athena.allinone.DBConnectionInfo;
import me.ele.jarch.athena.allinone.DBRole;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.scheduler.MulScheduler.PayloadType;
import me.ele.jarch.athena.util.AthenaConfig;
import me.ele.jarch.athena.util.ZKCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class MulSchedulerTest {
    private static final Logger logger = LoggerFactory.getLogger(MulSchedulerTest.class);

    //    @Test
    public void MulSchedulerPerformence() throws InterruptedException {
        AthenaConfig.getInstance().setMulschedulerWorkerCount(1);
        AthenaConfig.getInstance().setMulschedulerCapacity(10240);
        //        MulScheduler ms = MulScheduler.getInstance();
        DBConnectionInfo info =
            new DBConnectionInfo("eos_group", "a", "127.0.0.1", 2, "a", "a", DBRole.SLAVE, "test",
                true);

        final ZKCache zk1 = new ZKCache("eos_group");
        zk1.setZkCfg(Constants.MAX_ACTIVE_DB_SESSIONS, "100");
        zk1.setZkCfg(Constants.MAX_ACTIVE_DB_TRANS, "100");
        zk1.setZkCfg(Constants.MAX_QUEUE_SIZE, "10000000");
        final ZKCache zk2 = new ZKCache("eus_group");
        zk2.setZkCfg(Constants.MAX_ACTIVE_DB_SESSIONS, "1");
        zk2.setZkCfg(Constants.MAX_ACTIVE_DB_TRANS, "100");
        zk2.setZkCfg(Constants.MAX_QUEUE_SIZE, "10000000");
        final Scheduler sch1 = (new Scheduler(info, zk1, "eos_group"));
        //        final Scheduler sch2 = (new Scheduler(info, zk2, "eus_group"));
        List<Scheduler> ss = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            DBConnectionInfo tmpinfo =
                new DBConnectionInfo("test", "a", "127.0.0.1", 2, "a", "a", DBRole.SLAVE, "test",
                    true);
            final ZKCache zk = new ZKCache("exs_group");
            zk.setZkCfg(Constants.MAX_ACTIVE_DB_SESSIONS, "2");
            zk.setZkCfg(Constants.MAX_ACTIVE_DB_TRANS, "100");
            zk.setZkCfg(Constants.MAX_QUEUE_SIZE, "10000000");
            Scheduler s = new Scheduler(tmpinfo, zk, "exs_group");
            s.enqueue(new SqlSessionContext(null, "1.1.1.1"), PayloadType.QUERY, false);
            ss.add(s);
        }
        SqlSessionContext sc1 = new SqlSessionContext(null, "1.1.1.1");
        //        SqlSessionContext sc2 = new SqlSessionContext(null);
        long start = System.currentTimeMillis();
        List<Thread> ts = new ArrayList<>();
        for (int index = 0; index < 4; index++) {
            Thread t1 = new Thread(() -> {
                for (int j = 0; j < 100_000; j++) {
                    for (int i = 0; i < 100; i++) {
                        sch1.enqueue(sc1, PayloadType.TRX, true);
                    }
                    try {
                        while (sch1.getQueueBlockingTaskCount() > 300) {
                            Thread.sleep(1);
                        }
                    } catch (Exception e) {
                        logger.error(Objects.toString(e));
                        // e.printStackTrace();
                    }
                }
            });
            t1.setName("TEST" + index);
            ts.add(t1);
            t1.start();
        }
        Thread t2 = new Thread(() -> {
            SqlSessionContext tmpSession = new SqlSessionContext(null, "1.1.1.1");
            for (int j = 0; j < 100000; j++) {
                int id = j % 100;
                ss.get(id).enqueue(tmpSession, PayloadType.TRX, true);
                try {
                    Thread.sleep(1);
                } catch (Exception e) {
                    logger.error(Objects.toString(e));
                    // e.printStackTrace();
                }
            }
        });
        t2.setName("TEST_Other");
        t2.start();
        // t1.join();
        // t2.join();
        ts.stream().anyMatch(t -> {
            return t.isAlive();
        });
        while (true) {
            boolean alive = ts.stream().anyMatch(t -> {
                return t.isAlive();
            });
            if (!alive
                // && !t2.isAlive()
                && sch1.getQueueBlockingTaskCount() == 0) {
                break;
            }
            Thread.sleep(1000);
            System.out.println("current queue size is " + sch1.getQueueBlockingTaskCount());
        }
        System.out.println("finish ======================>" + (System.currentTimeMillis() - start));
    }
}
