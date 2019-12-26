package me.ele.jarch.athena.util.deploy;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import me.ele.jarch.athena.scheduler.DBChannelDispatcher;
import me.ele.jarch.athena.sql.seqs.GlobalId;
import me.ele.jarch.athena.util.deploy.dalgroupcfg.DalGroupConfig;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;

public class DalGroupConfigTest {

    private InputStream getFileStream(String fileName) {
        return getClass().getClassLoader().getResourceAsStream(fileName);
    }

    @Test public void jsonStr2ObjTest() throws IOException {
        ObjectMapper objectMapper =
            new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        DalGroupConfig dalGroupConfig = objectMapper
            .readValue(getFileStream("dalgroup-new_eos_main_group.json"), DalGroupConfig.class);
        Assert.assertEquals("new_eos_main_group", dalGroupConfig.getName());
        Assert.assertEquals(dalGroupConfig.getDbInfos().get(0).getGroup(),
            "new_eos_eleme-zeus_eos_group");
        Assert.assertEquals(dalGroupConfig.getDbInfos().get(0).getPort(), 4406);
        Assert.assertEquals(
            dalGroupConfig.getDbInfos().get(0).getAdditionalDalCfg().get("dal_sequence_table"),
            "dal_sequence");
        Assert.assertEquals(
            dalGroupConfig.getDbInfos().get(0).getAdditionalSessionCfg().get("TimeZone"), "UTC");

        Assert.assertEquals(dalGroupConfig.getShardingTables().size(), 2);
    }

    /**
     * not time to fix, temp disable
     *
     * @throws IOException
     */
    @Test(enabled = false) public void dalGroupCfgLoaderTest() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        DalGroupConfig dalGroupConfig = objectMapper
            .readValue(getFileStream("dalgroup-new_eos_main_group.json"), DalGroupConfig.class);
        DBChannelDispatcher.updateDispatcher(dalGroupConfig);
        DBChannelDispatcher dbChannelDispatcher =
            DBChannelDispatcher.getHolders().get(dalGroupConfig.getName());
        Assert.assertEquals(dbChannelDispatcher.getDalGroup().getName(), "new_eos_main_group");
        long dbCnt = dbChannelDispatcher.getDbGroups().values().stream()
            .mapToLong(dbGroup -> dbGroup.getDbGroupUpdater().getAllInfos().values().size()).sum();
        Assert.assertEquals(10, dbCnt);
        Assert.assertEquals(dbChannelDispatcher.getShardingRouter().getAllShardingDbGroups().size(),
            4);
        Assert.assertEquals(
            dbChannelDispatcher.getShardingRouter().getAllOriginShardedTables().size(), 4);
        Assert.assertEquals(2,
            dbChannelDispatcher.getShardingRouter().getMappingKey("eleme_order").size());
        Assert.assertEquals(
            dbChannelDispatcher.getShardingRouter().getMappingKey("eleme_order").get("user_id")
                .getMappingRuleBySepName("order_id").getTablePrefix(),
            "dal_mapping_order_id_user_id_");
        Assert.assertEquals(0, GlobalId.getGlobalIdsByDALGroup("new_eos_main_group").size());
        Assert.assertEquals("contract_id", GlobalId.getGlobalIdBySeqName("contract_id").bizName);

        DalGroupConfig newDalGroupConfig = objectMapper
            .readValue(getFileStream("dalgroup-new_eos_main_group_update.json"),
                DalGroupConfig.class);
        DBChannelDispatcher.updateDispatcher(newDalGroupConfig);
        DBChannelDispatcher newDispatcher =
            DBChannelDispatcher.getHolders().get(dalGroupConfig.getName());
        Assert.assertEquals(newDispatcher.getDalGroup().getName(), "new_eos_main_group");
        long newDbCnt = newDispatcher.getDbGroups().values().stream()
            .mapToLong(dbGroup -> dbGroup.getDbGroupUpdater().getAllInfos().values().size()).sum();
        Assert.assertEquals(6, newDbCnt);
        Assert.assertEquals(2, newDispatcher.getShardingRouter().getAllShardingDbGroups().size());
        Assert
            .assertEquals(newDispatcher.getShardingRouter().getAllOriginShardedTables().size(), 2);
        Assert.assertFalse(
            newDispatcher.getShardingRouter().isMappingShardingTable("tb_shipping_order"));
        Assert.assertEquals(
            newDispatcher.getShardingRouter().getComposedKey("tb_shipping_order").getSeq_name(),
            "composed_seq");
        Assert.assertEquals(
            newDispatcher.getShardingRouter().getComposedKey("tb_shipping_order").getColumns()
                .size(), 1);
        Assert.assertEquals(1, GlobalId.getGlobalIdsByDALGroup("new_eos_main_group").size());
        Assert.assertEquals("shipping", GlobalId.getGlobalIdBySeqName("shipping").bizName);
    }

    @AfterClass public void tearDown() {
        DBChannelDispatcher.getHolders().forEach((k, dh) -> {
            DBChannelDispatcher.updateDispatcher(buildEmptyDalGroupCfg(k));
        });
    }

    private DalGroupConfig buildEmptyDalGroupCfg(String name) {
        DalGroupConfig dalGroupConfig = new DalGroupConfig(name);
        dalGroupConfig.setHomeDbGroupName("");
        return dalGroupConfig;
    }
}
