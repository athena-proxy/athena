package me.ele.jarch.athena.util.config;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

/**
 * Created by jinghao.wang on 17/2/20.
 */
public class GlobalIdConfigTest {
    GlobalIdConfig config;

    @BeforeMethod public void init() throws FileNotFoundException {
        config = GlobalIdConfig
            .load(getClass().getClassLoader().getResourceAsStream("dal-globalids.yml"));
    }

    @Test public void testLoad() {
        //total schema
        assertEquals(config.getSchemas().size(), 3);
        //hongbao
        assertEquals(config.getSchemas().get(0).getZones().size(), 2);
        //notify
        assertEquals(config.getSchemas().get(1).getZones().size(), 2);
        //order_id
        assertEquals(config.getSchemas().get(2).getZones().size(), 3);
    }

    @Test public void testHongbaoSchema() {
        GlobalIdConfig.GlobalIdSchema hongbaoSchema = config.getSchemas().get(0);

        GlobalIdConfig.GlobalIdSchema expectedSchema = new GlobalIdConfig.GlobalIdSchema();

        List<GlobalIdConfig.Zone> expectedZones = new ArrayList<GlobalIdConfig.Zone>();
        GlobalIdConfig.Zone zoneXg = new GlobalIdConfig.Zone();
        zoneXg.setZone("xg");
        zoneXg.setSeq_name_zone("hongbao_xg");
        zoneXg.setIn_use(true);
        zoneXg.setMin_seed(new Long("97656250000000"));
        zoneXg.setMin_globalid(new Long("100000000000000000"));
        zoneXg.setMax_seed(new Long("195312499999999"));
        zoneXg.setMax_globalid(new Long("199999999999999999"));

        GlobalIdConfig.Zone zoneWg = new GlobalIdConfig.Zone();
        zoneWg.setZone("wg");
        zoneWg.setSeq_name_zone("hongbao_wg");
        zoneWg.setIn_use(true);
        zoneWg.setMin_seed(new Long("292968750000000"));
        zoneWg.setMin_globalid(new Long("300000000000000000"));
        zoneWg.setMax_seed(new Long("390624999999999"));
        zoneWg.setMax_globalid(new Long("399999999999999999"));

        expectedZones.add(zoneXg);
        expectedZones.add(zoneWg);

        expectedSchema.setBiz_name("hongbao");
        expectedSchema.setGenerator("composed_seq");
        expectedSchema.setRecycled(false);

        List<String> params = new ArrayList<String>();
        params.add("user_id");
        expectedSchema.setParams(params);
        expectedSchema.setSeq_name("hongbao");
        expectedSchema.setZones(expectedZones);

        assertEquals(hongbaoSchema.toString(), expectedSchema.toString());
    }

    @Test public void testNotifySchema() {
        GlobalIdConfig.GlobalIdSchema notifySchema = config.getSchemas().get(1);
        assertEquals(notifySchema.getBiz_name(), "notify");
        assertEquals(notifySchema.getSeq_name(), "notify");
        assertEquals(notifySchema.getGenerator(), "common_seq");
        assertEquals(notifySchema.getParams().size(), 0);
        assertFalse(notifySchema.getZones().get(1).isIn_use());
    }

    @Test public void testOrderIdSchema() {
        GlobalIdConfig.GlobalIdSchema orderIdSchema = config.getSchemas().get(2);
        assertEquals(orderIdSchema.getBiz_name(), "order_id");
        assertEquals(orderIdSchema.getSeq_name(), "eleme_order_orderid_seq");
        assertEquals(orderIdSchema.getGenerator(), "order_id");
        assertEquals(orderIdSchema.getParams().size(), 2);
        assertEquals(orderIdSchema.getZones().get(2).getGroups().get(0), "eos_main_group");
    }

}
