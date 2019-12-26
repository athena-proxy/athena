package me.ele.jarch.athena.server.pool;

import me.ele.jarch.athena.allinone.DBConnectionInfo;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;

/**
 * Created by Dal-Dev-Team on 17/2/25.
 */
public class PGServerSessionTest {
    @Test public void testCalcOverDue() throws Exception {
        DBConnectionInfo dbConnectionInfo = DBConnectionInfo.parseDBConnectionString(
            "eos   eleme        10.0.0.1 13306 master 0 eleme_user dGVzdAo= ");

        ServerSession serverSession = new PGServerSession(dbConnectionInfo,
            ServerSessionPoolCenter.getInst().getServerSessionPool(dbConnectionInfo, false));
        long value = serverSession.overdue;
        assertTrue(value >= PGServerSession.DEFAULT_OVERDUE_IN_MINUTES * 1000 * 60
            && value <= PGServerSession.DEFAULT_OVERDUE_IN_MINUTES * 3 * 1000 * 60);

        for (int i = 0; i < 10000; i++) {
            DBConnectionInfo dbConnectionInfo1 = DBConnectionInfo.parseDBConnectionString(
                "eos   eleme        10.0.0.1 13306 master 0 eleme_user dGVzdAo= pg_db_overdue_in_minutes=240");
            ServerSession serverSession1 = new PGServerSession(dbConnectionInfo1,
                ServerSessionPoolCenter.getInst().getServerSessionPool(dbConnectionInfo1, false));
            long value1 = serverSession1.overdue;
            assertTrue(value1 <= 720 * 1000 * 60 && value1 >= 240 * 1000 * 60);
        }
    }

}
