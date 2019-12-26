package me.ele.jarch.athena.sharding;

import me.ele.jarch.athena.scheduler.DBChannelDispatcher;
import me.ele.jarch.athena.sharding.ShardingConfig.ComposedKey;
import me.ele.jarch.athena.sharding.ShardingConfig.Rule;
import me.ele.jarch.athena.util.deploy.dalgroupcfg.DalGroupConfig;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.collections.CollectionUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

import static org.testng.Assert.*;

public class ShardingConfigTest {
    ShardingConfig cfg;

    @BeforeClass public void before() throws Exception {
        cfg =
            ShardingConfig.load(getClass().getClassLoader().getResource("sharding.yml").getFile());
        ShardingConfig.shardingTableMap.forEach((dalgroup, sTables) -> {
            DBChannelDispatcher.updateDispatcher(buildDalGroupCfg(dalgroup, false));
        });
    }

    public void tearDown() {
        ShardingConfig.shardingTableMap.forEach((dalgroup, sTables) -> {
            DBChannelDispatcher.updateDispatcher(buildDalGroupCfg(dalgroup, true));
        });
    }

    @Test public void mappingKey() {
        //assert mapping key 存在性
        for (int i = 0; i < 1; i++) {
            assertTrue(cfg.getSchemas().get(i).getMapping_key() == null);
        }
        assertTrue(cfg.getSchemas().get(1).getMapping_key() != null);

        //assert mapping key 正确性
        ShardingConfig.MappingKey mappingKey = cfg.getSchemas().get(1).getMapping_key();
        List<ShardingConfig.MappingRule> mappingRuleList = mappingKey.getMappingRuleValues();
        assertEquals(mappingRuleList.size(), 1);
        Iterator<ShardingConfig.MappingRule> iterator = mappingRuleList.iterator();
        ShardingConfig.MappingRule entry = iterator.next();
        assertEquals(entry.getSepColumnName(), "platform_tracking_id");
        assertEquals(entry.getTablePrefix(), "tb_shipping_order_tracking_id_mapping_dal_");
        assertEquals(mappingKey.getColumn(), "retailer_id");
    }

    @Test public void newStyleMappingKey() {
        final String group = "apollo_new_group";
        final String table = "tb_shipping_order";
        ShardingRouter router = ShardingRouter.getShardingRouter(group);
        assertTrue(router.isShardingTable(table));
        assertTrue(router.isMappingShardingTable(table));
        LinkedHashMap<String, ShardingConfig.MappingKey> mkeys = router.getMappingKey(table);
        assertEquals(mkeys.size(), 1);
        assertTrue(mkeys.containsKey("retailer_id"));
        ShardingConfig.MappingKey mappingKey = mkeys.get("retailer_id");
        assertEquals(mappingKey.getMappingRuleValues().size(), 1);
        assertEquals(mappingKey.getMappingColumns().get("platform_tracking_id").get(0),
            "platform_tracking_id");
        assertFalse(CollectionUtils.hasElements(mappingKey.getMappingColumns().get("abc_id")));

    }

    @Test public void mappingRouter() {
        String group = "apollo_group";
        ShardingRouter shardingRouter = ShardingRouter.getShardingRouter(group);
        String table = "tb_shipping_order";

        assertTrue(shardingRouter.isShardingTable(table));
        assertTrue(shardingRouter.isMappingShardingTable(table));

        assertEquals(shardingRouter.getShardingKeys(table).size(), 1);
        assertEquals(shardingRouter.getShardingKeys(table).get(0), "retailer_id");

        String name4MappingKey = "platform_tracking_id";
        String value4MappingKey = "987654321";
        ShardingConfig.MappingKey mappingKey =
            shardingRouter.getMappingKey(table).get("retailer_id");
        ShardingTable pairRid = shardingRouter
            .getMappingTableByMappingKey(table, mappingKey, name4MappingKey, value4MappingKey);
        assertEquals(pairRid.table, "tb_shipping_order_tracking_id_mapping_dal_410");
        assertEquals(pairRid.database, "apollo_group4");


        String tableError = "tb_shipping_order_error";
        assertFalse(shardingRouter.isShardingTable(tableError));
    }

    @Test public void virtualRouter() {
        String group = "apollo_group";
        ShardingRouter shardingRouter = ShardingRouter.getShardingRouter(group);

        String virtualTable = "tb_shipping_order_tracking_id_mapping_dal_";
        String column = "platform_tracking_id";

        assertTrue(shardingRouter.isShardingTable(virtualTable));
        assertFalse(shardingRouter.isMappingShardingTable(virtualTable));

        assertEquals(shardingRouter.getShardingKeys(virtualTable),
            Collections.singletonList(column));


        ShardingConfig.ComposedKey ckey = shardingRouter.getComposedKey(virtualTable);
        assertEquals(ckey.getColumn(), column);
        assertEquals(ckey.getShardingColumns().get(0), column);
        assertEquals(ckey.getColumns(), Collections.singletonList("platform_tracking_id"));
        assertEquals(ckey.extractValue(column, "123456789"), 53);
        assertEquals(ckey.extractValue(column, "987654321"), 821);
        assertEquals(ckey.getSeq_name(), "mapping");

        ShardingTable pairRid =
            shardingRouter.getShardingTableByComposedKey(virtualTable, column, "9999");
        assertEquals(pairRid.table, "tb_shipping_order_tracking_id_mapping_dal_288");
        assertEquals(pairRid.database, "apollo_group3");

        assertEquals(ckey.getShardingId("0"), "48");
    }

    @Test public void getAllShardingTable() {
        String group = "apollo_group";
        ShardingRouter shardingRouter = ShardingRouter.getShardingRouter(group);
        List<ShardingTable> tables = shardingRouter.getAllShardingTable("tb_shipping_order");
        assertEquals(tables.size(), 512);
        assertTrue(tables.get(0).table.startsWith("tb_shipping_order"));
        assertTrue(tables.get(0).database.startsWith("apollo_group"));
    }

    @Test public void getShardingTableByIndex() {
        String group = "new_eos_main_group";
        ShardingRouter shardingRouter = ShardingRouter.getShardingRouter(group);
        ShardingTable result = shardingRouter.getShardingTableByIndex("eleme_order", 1);
        assertEquals(result.table, "eleme_order_uid_1");
        result = shardingRouter.getShardingTableByIndex("eleme_order", 7);
        assertEquals(result.table, "eleme_order_uid_7");
    }

    @Test public void testMulKeyHash() {
        String group = "eos_mulkeyhash__group";
        ShardingRouter shardingRouter = ShardingRouter.getShardingRouter(group);
        String table = "mulkeyhash_table";
        String key = "user_id,restaurant_id";
        String value0 = "0";
        String value1 = "1";
        ShardingTable pair0 = shardingRouter.getShardingTable(table, key, value0, value1);
        ShardingTable pair1 = shardingRouter.getShardingTable(table, key, value1, value0);
        assertEquals(pair0.table, pair1.table);
        assertEquals(pair0.database, pair1.database);

        ComposedKey ckey = shardingRouter.getComposedKey(table);
        assertEquals(ckey.extractValue("user_id,restaurant_id",
            String.valueOf(11L * 1024L * 1024L + 55 * 1024 + 997)), 997);
        assertEquals(ckey.getShardingColumns().get(0), "user_id,restaurant_id");
        ShardingTable pairRid = shardingRouter
            .getShardingTableByComposedKey(table, "user_id,restaurant_id",
                String.valueOf(11L * 1024L * 1024L + 521L * 1024L + 110));
        assertEquals(pairRid.table, "shd_eleme_order_rid12");
        assertEquals(pairRid.database, "eosgroup109");
    }


    @Test public void testLongestCommonPrefix() {
        List<String> strs = new ArrayList<>();
        strs.add("sku_food_id_mapping_dal_1");
        strs.add("sku_food_id_mapping_dal_a");
        strs.add("sku_food_id_mapping_dal_[2-100]");
        strs.add("sku_food_id_mapping_dal_1000");
        assertEquals(ShardingConfig.longestCommonPrefix(strs), "sku_food_id_mapping_dal_");
    }

    @Test public void testRuleparse() throws FileNotFoundException {
        Rule r = new Rule();
        r.setColumn("testCloumn");
        Map<String, String> routes = new HashMap<>();
        routes.put("prefix_postfix", "g0");
        routes.put("prefix_[1-2]_postfix", "g1");
        routes.put("prefix_[3]_postfix", "g2");
        routes.put("prefix_[4 - 5]_postfix", "g3");
        routes.put("prefix_[6-7]", "g4");
        routes.put("prefix_[8]", "g5");
        routes.put("prefix_[9 - 10]", "g6");
        routes.put("prefix_[11-12,12-13]", "g7");
        routes.put("prefix_[14-20-2, 21,22-23]", "g8");
        r.setDb_routes(routes);
        r.setHash_file("sharding_hash_test.yml");
        r.parse(
            new File(getClass().getClassLoader().getResource("sharding_hash_test.yml").getFile())
                .getParent());
        assertEquals(r.getExpandDbRoutes().get("prefix_2_postfix"), "g1");
        assertEquals(r.getExpandDbRoutes().get("prefix_postfix"), "g0");
        assertEquals(r.getExpandDbRoutes().get("prefix_3_postfix"), "g2");
        assertEquals(r.getExpandDbRoutes().get("prefix_9"), "g6");
        assertEquals(r.getExpandDbRoutes().get("prefix_11"), "g7");
        assertEquals(r.getExpandDbRoutes().get("prefix_13"), "g7");
        assertEquals(r.getExpandDbRoutes().get("prefix_16"), "g8");
        assertNull(r.getExpandDbRoutes().get("prefix_17"));
        assertEquals(r.getExpandDbRoutes().get("prefix_21"), "g8");
    }

    @Test public void testNonMappingShardingTableCheck() throws Exception {
        final String nonMappingShardingTable = "non_mapping_sharding_table";
        final String group = "new_eos_main_group";
        final ShardingRouter shardingRouter = ShardingRouter.getShardingRouter(group);
        assertFalse(shardingRouter.isMappingShardingTable(nonMappingShardingTable));

        final String shardingTable = "common_self_table";
        final String simpleShardingGroup = "eos_main_group";
        final ShardingRouter simpleShardingRouter =
            ShardingRouter.getShardingRouter(simpleShardingGroup);
        assertTrue(simpleShardingRouter.isShardingTable(shardingTable));
        assertFalse(simpleShardingRouter.isMappingShardingTable(shardingTable));
    }

    @Test public void testNewOrderShardingRules() {
        final String group = "new_eos_main_group";
        final ShardingConfig.Schema schema = cfg.getSchemas().get(3);
        assertTrue(Objects.nonNull(schema.getComposed_key()));
        assertEquals(schema.getMapping_key(), null);
        ShardingRouter shardingRouter = ShardingRouter.getShardingRouter(group);
        final String table = "eleme_order";
        LinkedHashMap<String, ShardingConfig.MappingKey> ruleNameWithMappingKey =
            shardingRouter.getMappingKey(table);
        Set<String> ruleNames = new LinkedHashSet<>();
        ruleNames.add("user_id");
        ruleNames.add("restaurant_id");
        assertEquals(ruleNameWithMappingKey.keySet(), ruleNames);

        assertTrue(shardingRouter.isShardingTable("dal_mapping_order_id_user_id_"));
        assertTrue(shardingRouter.isShardingTable("dal_mapping_order_id_restaurant_id_"));

        String value4OrderId = "123";
        String name4OrderId = "order_id";
        ShardingTable uIdDimMappingTable = shardingRouter
            .getMappingTableByMappingKey(table, ruleNameWithMappingKey.get("user_id"), name4OrderId,
                value4OrderId);
        assertEquals(uIdDimMappingTable.database, "new_eos_eleme-zeus_eos_group2");
        assertEquals(uIdDimMappingTable.table, "dal_mapping_order_id_user_id_4");

        ShardingTable rIdDimMappingTable = shardingRouter
            .getMappingTableByMappingKey(table, ruleNameWithMappingKey.get("restaurant_id"),
                name4OrderId, value4OrderId);
        assertEquals(rIdDimMappingTable.database, "new_eos_eleme-zeus_eos_group4");
        assertEquals(rIdDimMappingTable.table, "dal_mapping_order_id_restaurant_id_4");
    }

    private static DalGroupConfig buildDalGroupCfg(String name, boolean isEmpty) {
        DalGroupConfig dalGroupConfig = new DalGroupConfig(name);
        dalGroupConfig.setHomeDbGroupName(name);
        if (!isEmpty) {
            dalGroupConfig.updateShardingCfg();
        }
        return dalGroupConfig;
    }

    @Test public void hashRange() {
        assertEquals(cfg.getSchemas().get(5).getRules().get(0).getHash_range(), 1024);
    }
}
