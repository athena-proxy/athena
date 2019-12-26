package me.ele.jarch.athena.sharding.sql;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * 集中存储sharding相关列及其对应的值
 * Created by jinghao.wang on 2017/10/27.
 */
class ShardingValues {
    public final Map<String, Set<String>> composeColumnNameWithValues = new HashMap<>(2);
    public final Map<String, Set<String>> shardingKeyNameWithValues = new HashMap<>(2);
    public final Map<String, Set<String>> mappingKeyNameWithValues = new HashMap<>(2);

    public ShardingValues() {
    }
}
