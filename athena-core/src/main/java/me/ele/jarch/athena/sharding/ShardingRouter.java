package me.ele.jarch.athena.sharding;

import me.ele.jarch.athena.sharding.ShardingConfig.ComposedKey;
import me.ele.jarch.athena.sharding.ShardingConfig.MappingKey;
import me.ele.jarch.athena.sharding.ShardingConfig.Rule;
import me.ele.jarch.athena.sql.seqs.generator.ComposedGenerator;
import me.ele.jarch.athena.sql.seqs.generator.Generator;
import me.ele.jarch.athena.util.common.NaturalOrderComparator;
import me.ele.jarch.athena.util.deploy.dalgroupcfg.DalGroupConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class ShardingRouter {
    private static final Logger logger = LoggerFactory.getLogger(ShardingRouter.class);
    private static volatile Map<String, ShardingRouter> shardingRouters = new ConcurrentHashMap<>();
    public static final ShardingRouter defaultShardingRouter = new ShardingRouter();

    //table -> sharding_keys, eg: eleme_order -> [user_id, restaurant_id]
    private final Map<String, List<String>> tableKeys = new HashMap<String, List<String>>();

    // not like shardingTables
    // the index of ShardingTable[]  is from 0 to hash_count such as 1024
    // (eleme_order,user_id) -> [0:(eleme_order_uid_0,eos_main_group1),
    //                           1:(eleme_order_uid_0,eos_main_group1),
    //                           ...
    //                           100:(eleme_order_uid_11,eos_main_group2),
    //                          ...
    //                           1023:(eleme_order_uid_119,eos_main_group12),
    //                          ]
    // (eleme_order,restaurant_id) -> ...
    private final Map<PairTableKey, ShardingTable[]> routers =
        new HashMap<ShardingRouter.PairTableKey, ShardingTable[]>();

    // @format:off
    // eleme_order -> {
    // column : id, // 如果以逗号分隔,那么表示是复合composed_key,如team_id和org_id(线上未曾使用)
    // columns : // 如果以逗号分隔,那么是id.split(",")(线上未曾使用)
    // seq_name : order_id(或composed_seq等)
    // compose_rules : [user_id,restaurant_id]
    // extractors : {user_id->ComposedKeyExtractor,restaurant_id->ComposedKeyExtractor}
    // shardingColumns : [user_id,restaurant_id]
    // g : Generator;
    // }
    // @format:on
    private final Map<String, ComposedKey> composedkeys =
        new HashMap<String, ShardingConfig.ComposedKey>();
    /**
     * @formatter:off 一个ShardingRouter关联一个DALGroup, 一个 DALGroup可能有多张sharding表,每张sharding 表都可能存在mapping key.
     * 大部分使用mapping表规则的sharding只有一个mapping key, 但是存在少入sharding表, 二维sharding, 2个mapping key.
     * eg:
     * 二维sharding mappping
     * new_eleme_order : (user_id: mapping_key)
     * : (restaurant_id: mapping_key)
     * 一维sharding mapping
     * commodity_group : (shop_id: mapping_key)
     * commodity_group_has_item : (shop_id: mapping_key)
     * commodity_item : (shop_id: mapping_key)
     * @formatter:on
     */
    private final Map<String, LinkedHashMap<String, MappingKey>> mappingKeys = new HashMap<>();

    // (eleme_order,user_id) -> [(eleme_order_uid_0,eos_main_group1),
    //                           (eleme_order_uid_1,eos_main_group1),
    //                          ...
    //                           (eleme_order_uid_10,eos_main_group2),
    //                          ...
    //                          ]
    // (eleme_order,restaurant_id) -> ...
    // The index of List<>  is from 0 to sharding_count
    private final Map<PairTableKey, List<ShardingTable>> shardingTables =
        new HashMap<ShardingRouter.PairTableKey, List<ShardingTable>>();


    static public ShardingRouter getShardingRouter(String group) {
        if (group.indexOf("__sub__") != -1) {
            // share the sharding db for multiple group
            int index = group.indexOf("__sub__");
            String mainName = group.substring(0, index);
            return shardingRouters
                .getOrDefault(group, shardingRouters.getOrDefault(mainName, defaultShardingRouter));
        }
        return shardingRouters.getOrDefault(group, defaultShardingRouter);
    }


    public static void updateShardRouter(DalGroupConfig dalGroupConfig) {
        if (dalGroupConfig.getShardingTables().isEmpty()) {
            shardingRouters.remove(dalGroupConfig.getName());
            return;
        }
        ShardingRouter router = new ShardingRouter();
        boolean isValid = dalGroupConfig.getShardingTables().stream()
            .flatMap(shardingTable -> shardingTable.getRules().stream()).allMatch(Rule::validate);
        if (!isValid) {
            logger.error("Sharding config for dalgroup {} is not valid, skipping loading",
                dalGroupConfig.getName());
            throw new RuntimeException("Sharding config for dalgroup " + dalGroupConfig.getName()
                + " is not valid, skipping loading");
        }
        dalGroupConfig.getShardingTables().forEach(sTable -> {
            LinkedHashMap<Rule, MappingKey> ruleWithMappingKey = new LinkedHashMap<>();
            sTable.getRules().forEach((Rule rule) -> {
                final MappingKey mappingKey = rule.getMapping_key();
                if (Objects.nonNull(mappingKey)) {
                    ruleWithMappingKey.put(rule, mappingKey);
                }
                ShardingTable[] tableMapping = buildShardTables(rule);
                List<ShardingTable> sTableList = new ArrayList<>();
                rule.getExpandDbRoutes().forEach((table, db) -> {
                    sTableList.add(new ShardingTable(table, db));
                });
                NaturalOrderComparator comparator = new NaturalOrderComparator();
                sTableList.sort((st1, st2) -> comparator.compare(st1.table, st2.table));
                PairTableKey pairTableKey = new PairTableKey(sTable.getTable(), rule.getColumn());
                router.routers.put(pairTableKey, tableMapping);
                router.shardingTables.put(pairTableKey, sTableList);
            });
            List<String> keys =
                sTable.getRules().stream().map(Rule::getColumn).collect(Collectors.toList());
            if (sTable.getComposedKey() != null && keys.size() > 0) {
                router.composedkeys.put(sTable.getTable(), sTable.getComposedKey());
            }
            router.tableKeys.put(sTable.getTable(), keys);
            if (needAttachMappingKey(sTable.getComposedKey(), keys, ruleWithMappingKey)) {
                ruleWithMappingKey.forEach((rule, mappingKey) -> {
                    attachMappingKey(mappingKey, rule, sTable.getTable(), router);
                });
            }
        });
        shardingRouters.put(dalGroupConfig.getName(), router);
    }

    private static ShardingTable[] buildShardTables(Rule rule) {
        ShardingTable[] tableMapping = new ShardingTable[rule.getHash_range()];
        String[] valueRealTables = rule.getExpandTablesRoutes();
        Map<String, String> realTableDatabases = rule.getExpandDbRoutes();
        for (int i = 0; i < rule.getHash_range(); i++) {
            String table = valueRealTables[i];
            String database = realTableDatabases.get(table);
            ShardingTable pairDt = new ShardingTable(table, database);
            tableMapping[i] = pairDt;
        }
        return tableMapping;
    }

    private static boolean needAttachMappingKey(ComposedKey composedKey, List<String> ruleColumns,
        LinkedHashMap<Rule, MappingKey> ruleWithMappingKey) {
        if (Objects.isNull(composedKey))
            return false;
        if (ruleWithMappingKey.isEmpty()) {
            return false;
        }
        if (ruleColumns.size() != ruleWithMappingKey.size()) {
            return false;
        }
        return ruleWithMappingKey.values().stream().map(MappingKey::getColumn)
            .allMatch(ruleColumns::contains);
    }

    private static void attachMappingKey(MappingKey mappingKey, Rule rule, String table,
        ShardingRouter newShardingRouter) {
        newShardingRouter.mappingKeys.computeIfAbsent(table, k -> new LinkedHashMap<>())
            .put(rule.getColumn(), mappingKey);
        //构造映射表的虚拟路由
        PairTableKey originPTKey = new PairTableKey(table, rule.getColumn());
        mappingKey.getMappingRuleValues().forEach(mappingRule -> {
            PairTableKey virtualPTKey =
                new PairTableKey(mappingRule.getTablePrefix(), mappingRule.getSepColumnName());
            ShardingTable[] realTableMapping = newShardingRouter.routers.get(originPTKey);
            ShardingTable[] virtualTableMapping =
                buildVirtualTableMapping(realTableMapping, rule, mappingRule.getTablePrefix());
            newShardingRouter.routers.put(virtualPTKey, virtualTableMapping);

            List<ShardingTable> realTables = newShardingRouter.shardingTables.get(originPTKey);
            List<ShardingTable> virtualTables =
                buildVirtualShardingTables(realTables, rule.getTablePrefix(),
                    mappingRule.getTablePrefix());
            newShardingRouter.shardingTables.put(virtualPTKey, virtualTables);

            ComposedKey virtualComposedKey =
                buildVirtualComposedKey(mappingRule.getSepColumnName());

            newShardingRouter.composedkeys.put(mappingRule.getTablePrefix(), virtualComposedKey);

            newShardingRouter.tableKeys.put(mappingRule.getTablePrefix(),
                Collections.singletonList(mappingRule.getSepColumnName()));
        });
    }

    private static ShardingTable[] buildVirtualTableMapping(ShardingTable[] realTableMapping,
        Rule rule, String virtualTablePrefix) {
        Objects.requireNonNull(realTableMapping, "real table mapping array must be not null");
        ShardingTable[] tableMapping = new ShardingTable[rule.getHash_range()];
        for (int i = 0; i < realTableMapping.length; i++) {
            tableMapping[i] = new ShardingTable(
                realTableMapping[i].table.replace(rule.getTablePrefix(), virtualTablePrefix),
                realTableMapping[i].database);
        }
        return tableMapping;
    }

    private static List<ShardingTable> buildVirtualShardingTables(
        List<ShardingTable> realShardingTables, String realTablePrefix, String virtualTablePrefix) {
        Objects.requireNonNull(realShardingTables, "real shardingTable list must be not null");
        List<ShardingTable> tables = realShardingTables.stream().map(
            shardingTable -> new ShardingTable(
                shardingTable.table.replace(realTablePrefix, virtualTablePrefix),
                shardingTable.database)).collect(Collectors.toList());
        return tables;
    }

    private static ComposedKey buildVirtualComposedKey(String column) {
        ComposedKey virtualComposedKey = new ComposedKey();
        virtualComposedKey.setColumn(column);
        virtualComposedKey.setSeq_name("mapping");
        virtualComposedKey.setCompose_rules(Collections.singletonList(column));
        virtualComposedKey.setExtractors(
            Collections.singletonMap(column, (keyValue) -> Generator.getHashCode(keyValue)));
        virtualComposedKey.setShardingColumns(Collections.singletonList(column));
        return virtualComposedKey;
    }

    public boolean isShardingTable(final String tableName) {
        return tableKeys.containsKey(tableName);
    }

    public boolean isMappingShardingTable(final String tableName) {
        return mappingKeys.containsKey(tableName);
    }

    public List<String> getShardingKeys(final String tableName) {
        return tableKeys.get(tableName) == null ?
            new ArrayList<String>() :
            tableKeys.get(tableName);
    }

    public ShardingTable getShardingTable(String tableName, String key, String... value) {
        int hashCode = -1;
        if (key.split(",").length > 1) {
            hashCode = (int) ComposedGenerator.getHashCodeValue(Arrays.asList(value));
        } else {
            hashCode = (int) Generator.getHashCode(value[0]);
        }
        return getShardingTableWithHashCode(tableName, key, hashCode);
    }

    protected ShardingTable getShardingTableWithHashCode(String tableName, String key, int value) {
        ShardingTable[] mapping = routers.get(new PairTableKey(tableName, key));
        return mapping == null ? null : mapping[value];
    }

    public ComposedKey getComposedKey(final String tableName) {
        return composedkeys.get(tableName);
    }

    public LinkedHashMap<String, MappingKey> getMappingKey(final String tableName) {
        return mappingKeys.get(tableName);
    }

    public List<String> getMappingColumnsByColumnList(final List<String> columns) {
        List<String> columnList = new ArrayList<>();
        mappingKeys.values().stream()
            .anyMatch(mappingKeyMap -> mappingKeyMap.values().stream().anyMatch(oneMappingKey -> {
                if (oneMappingKey == null) {
                    return false;
                }
                for (List<String> mappingKeyColumn : oneMappingKey.getMappingColumns().values()) {
                    if (mappingKeyColumn.size() == columns.size() && mappingKeyColumn
                        .containsAll(columns)) {
                        columnList.addAll(mappingKeyColumn);
                        return true;
                    }
                }
                return false;
            }));
        return columnList;
    }

    public ShardingTable getShardingTableByComposedKey(String tableName, String key, String value) {
        long eValue = composedkeys.get(tableName).extractValue(key, value);
        return getShardingTableWithHashCode(tableName, key, (int) eValue);
    }

    /**
     * 通过mapping 列的名字和值,获取对应的shardingTable
     *
     * @param tableName  需要sharding的小写表名
     * @param mappingKey 使用的MappingKey对象
     * @param key        映射表中作为key的列名
     * @param value      key的值
     * @return 映射表的ShardingTable
     */
    public ShardingTable getMappingTableByMappingKey(String tableName, MappingKey mappingKey,
        String key, String value) {
        if (Objects.isNull(mappingKey)) {
            return null;
        }
        long shardingId = Generator.getHashCode(String.valueOf(value));
        String column = mappingKey.getColumn();
        ShardingTable t = getShardingTableWithHashCode(tableName, column, (int) shardingId);
        if (Objects.isNull(t)) {
            return null;
        }
        String mappingLogicTable = mappingKey.getMappingRuleBySepName(key).getTablePrefix();
        String replaceTable = mappingKey.getReplaceTable();
        String table = t.table.replace(replaceTable, mappingLogicTable);
        return new ShardingTable(table, t.database);
    }

    /**
     * 返回第一维的所有sharding表
     *
     * @param tableName
     * @return
     */
    public List<ShardingTable> getAllShardingTable(String tableName) {
        String key = getShardingKeys(tableName).get(0);
        return shardingTables.get(new PairTableKey(tableName, key));
    }

    public Map<String, List<ShardingTable>> getAllShardingTablesOfAllDimensions(String tableName) {
        Map<String, List<ShardingTable>> resultMap = new HashMap<>();
        getShardingKeys(tableName).forEach(shardingKey -> {
            List<ShardingTable> shardingTablesOfOneDimension =
                shardingTables.get(new PairTableKey(tableName, shardingKey));
            if (!shardingTablesOfOneDimension.isEmpty()) {
                resultMap.put(shardingKey, shardingTablesOfOneDimension);
            }
        });

        return resultMap;
    }

    public ShardingTable getShardingTableByIndex(String tableName, int index) {
        String key = getShardingKeys(tableName).get(0);
        return shardingTables.get(new PairTableKey(tableName, key)).get(index);
    }

    /**
     * 同时支持计算1维, 2维, 2维mapping情况下的sharding id
     * 对于1维sharding,返回值是0-1023的值
     * 对于2维sharding,返回值是用户维和商户维hash值的合并，
     * 例如对于order id 9537374677,对应的shardingid是562469
     * 其中用户维hash是562,商户维hash是469
     * value可能为routingKey, 例如: shardid=2
     *
     * @param tableName
     * @param value     sharding id, 不一定是0-1023的值。
     * @return
     */
    public String computeShardingId(String tableName, String value) {
        return composedkeys.get(tableName).getShardingId(value);
    }

    /**
     * @param value eg: retailer_id, platform_tracking_id, out_trade_no
     * @return 0-1023 的sharding hash值
     * @formatter:off 根据传入的值纯粹的计算sharding hash
     * 传入的值可以是
     * 1. sharding key 的值 如retailer_id
     * 2. mapping key 的值 如platform_tracking_id
     * 3. 不嵌入hash信息的composed_id, 如out_trade_no
     * @formatter:on
     */
    public String computeShardingId(String value) {
        return String.valueOf(Generator.getHashCode(value));
    }

    public Set<String> getAllShardingDbGroups() {
        Set<String> dbgroups = new HashSet<>();
        this.shardingTables.values().stream().forEach(list -> {
            list.stream().forEach(st -> dbgroups.add(st.database));
        });
        return dbgroups;
    }

    // 获取类似于tb_shipping_order_1 -> (tb_shipping_order_1,apollo_1)的信息
    // 同时还带有原表的信息指向最后一个库,如tb_shipping_order -> (tb_shipping_order_511,apollo_4)
    public Map<String, ShardingTable> getAllShardedTables() {
        Map<String, ShardingTable> map = new TreeMap<>();
        shardingTables.forEach((pair, list) -> {
            map.put(pair.table, list.get(list.size() - 1));
            list.forEach(table -> {
                map.put(table.table, table);
            });
        });
        return map;
    }

    // 获取该router下所有可以sharding的表的原始表名,如tb_shipping_order
    public Set<String> getAllOriginShardedTables() {
        Set<String> set = new TreeSet<>();
        shardingTables.forEach((pair, x) -> set.add(pair.table));
        return set;
    }

    public Map<PairTableKey, Map<String, Integer>> getHealthCheckShardIdx() {
        Map<PairTableKey, Map<String, Integer>> shardIndexPerDBGroup = new HashMap<>();
        shardingTables.forEach((pairTableKey, shardingTables) -> {
            Map<String, Integer> shardTable = new HashMap<>();
            for (int i = 0; i < shardingTables.size(); i++) {
                if (shardTable.containsKey(shardingTables.get(i).database)) {
                    continue;
                }
                shardTable.put(shardingTables.get(i).database, i);
            }
            shardIndexPerDBGroup.put(pairTableKey, shardTable);
        });
        return shardIndexPerDBGroup;
    }

    public static void clear() {
        shardingRouters.clear();
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        ShardingRouter that = (ShardingRouter) o;

        if (!tableKeys.equals(that.tableKeys))
            return false;
        if (!isSameRouters(routers, that.routers))
            return false;
        if (!composedkeys.equals(that.composedkeys))
            return false;
        if (!mappingKeys.equals(that.mappingKeys))
            return false;
        return shardingTables.equals(that.shardingTables);
    }

    private boolean isSameRouters(Map<PairTableKey, ShardingTable[]> oldRouters,
        Map<PairTableKey, ShardingTable[]> newRouters) {
        if (oldRouters == newRouters)
            return true;

        if (oldRouters.size() != newRouters.size())
            return false;

        try {
            Iterator<Map.Entry<PairTableKey, ShardingTable[]>> i = oldRouters.entrySet().iterator();
            while (i.hasNext()) {
                Map.Entry<PairTableKey, ShardingTable[]> e = i.next();
                PairTableKey key = e.getKey();
                ShardingTable[] value = e.getValue();
                if (value == null) {
                    if (!(newRouters.get(key) == null && newRouters.containsKey(key)))
                        return false;
                } else {
                    if (!Arrays.equals(value, newRouters.get(key)))
                        return false;
                }
            }
        } catch (ClassCastException | NullPointerException unused) {
            return false;
        }

        return true;
    }

    @Override public int hashCode() {
        int result = tableKeys != null ? tableKeys.hashCode() : 0;
        result = 31 * result + (routers != null ? routers.hashCode() : 0);
        result = 31 * result + (composedkeys != null ? composedkeys.hashCode() : 0);
        result = 31 * result + (mappingKeys != null ? mappingKeys.hashCode() : 0);
        result = 31 * result + (shardingTables != null ? shardingTables.hashCode() : 0);
        return result;
    }

    public static final class PairTableKey {
        public String table;
        public String key;

        public PairTableKey(String table, String key) {
            this.table = table;
            this.key = key;
        }

        public PairTableKey() {
        }

        @Override public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((key == null) ? 0 : key.hashCode());
            result = prime * result + ((table == null) ? 0 : table.hashCode());
            return result;
        }

        @Override public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            PairTableKey other = (PairTableKey) obj;
            if (key == null) {
                if (other.key != null)
                    return false;
            } else if (!key.equals(other.key))
                return false;
            if (table == null) {
                if (other.table != null)
                    return false;
            } else if (!table.equals(other.table))
                return false;
            return true;
        }

    }
}
