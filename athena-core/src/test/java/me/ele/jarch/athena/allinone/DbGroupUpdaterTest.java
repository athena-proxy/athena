package me.ele.jarch.athena.allinone;

import me.ele.jarch.athena.scheduler.SchedulerGroup;
import me.ele.jarch.athena.util.ZKCache;
import me.ele.jarch.athena.util.deploy.DALGroup;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

public class DbGroupUpdaterTest {

    @Test public void update() {
        List<DBConnectionInfo> infos = new ArrayList<>();
        DBConnectionInfo info1 =
            new DBConnectionInfo("eos", "eleme", "10.0.0.1", 3306, "eleme_user",
                "6Q06PIELnH2mt4gpNuBlqw==", DBRole.MASTER, "0", true);
        info1.setReadOnly(false);
        DBConnectionInfo info2 =
            new DBConnectionInfo("eos", "eleme", "eos.eleme.net", 3306, "eleme_user",
                "6Q06PIELnH2mt4gpNuBlqw==", DBRole.SLAVE, "2", true);
        DBConnectionInfo info3 =
            new DBConnectionInfo("eus", "eleme", "eus.eleme.net", 3306, "eleme_user",
                "CEh2BIs2Yd1an+hlXAGzcw==", DBRole.MASTER, "0", true);
        infos.add(info1);
        infos.add(info2);
        infos.add(info3);
        ZKCache zkCache = new ZKCache("eos");
        DBGroup dbGroup = new DBGroup("eos", zkCache, new SchedulerGroup("eos", zkCache),
            new DALGroup("eos", "", "eos"));
        dbGroup.init();
        DbGroupUpdater dbGroupUpdater = dbGroup.getDbGroupUpdater();
        dbGroupUpdater.updateByDBCfgs(infos);
        Assert.assertEquals(1, dbGroup.getSlaves().size());
        Assert.assertEquals(2, dbGroupUpdater.getAllInfos().size());
        Assert.assertEquals(dbGroup.getMasterName(), "eos:0");
        infos.remove(info1);
        dbGroupUpdater.updateByDBCfgs(infos);
        Assert.assertNull(HeartBeatCenter.getInstance().getDBStatus(info1));
    }

}
