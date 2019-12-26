package me.ele.jarch.athena.util.deploy;

import me.ele.jarch.athena.sharding.ShardingConfig;
import me.ele.jarch.athena.util.GreySwitch;
import me.ele.jarch.athena.util.deploy.dalgroupcfg.ShardingTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.List;

import static org.testng.Assert.assertEquals;

public class LoadDalGroupCfgShardingTest {

    @BeforeClass public void before() throws Exception {
        GreySwitch.getInstance().setDalGroupCfgEnabled(true);
        ShardingConfig.load(getClass().getClassLoader().getResource("sharding.yml").getFile());
    }

    @Test public void testLoad() {
        ShardingTable shardingTable =
            ShardingConfig.shardingTableMap.get("new_eos_main_group").stream()
                .filter(s -> s.getTable().equals("eleme_order")).findFirst()
                .orElseThrow(() -> new RuntimeException("eleme_order was not loaded sucessfully"));
        assertEquals(shardingTable.getRules().stream().map(ShardingConfig.Rule::getColumn).count(),
            2);
        assertEquals(
            shardingTable.getRules().stream().filter(r -> r.getColumn().equals("user_id")).count(),
            1);
    }

    @Test public void testMappingKey() {
        ShardingTable mKeyTable = ShardingConfig.shardingTableMap.get("apollo_group").stream()
            .filter(s -> s.getTable().equals("tb_shipping_order")).findFirst().orElseThrow(
                () -> new RuntimeException("tb_shipping_order was not loaded sucessfully"));
        ShardingConfig.MappingKey mappingKey = mKeyTable.getRules().get(0).getMapping_key();
        List<ShardingConfig.MappingRule> mappingRules = mappingKey.getMappingRuleValues();
        assertEquals(mappingRules.size(), 1);
        Iterator<ShardingConfig.MappingRule> iterator = mappingRules.iterator();
        ShardingConfig.MappingRule entry = iterator.next();
        assertEquals(entry.getSepColumnName(), "platform_tracking_id");
        assertEquals(entry.getTablePrefix(), "tb_shipping_order_tracking_id_mapping_dal_");
        assertEquals(mappingKey.getColumn(), "retailer_id");
    }

    @AfterClass public void after() {
        GreySwitch.getInstance().setDalGroupCfgEnabled(false);

    }
}
