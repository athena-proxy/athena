package me.ele.jarch.athena.sharding.sql;

import com.alibaba.druid.sql.ast.SQLExpr;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jinghao.wang on 2017/10/26.
 */
class ShardingCondition {
    public final Map<String, SQLExpr> composeColumnNameWithSQLExpr = new HashMap<>(2);
    public final Map<String, SQLExpr> shardingKeyNameWithSQLExpr = new HashMap<>(2);
    /**
     * 用于收集where条件中=或in条件中出现过的mapping_rule名字及其语法节点
     * eg:
     * select * from tb_shipping_order where platform_tracking_id = 1 and state = 0
     * columns4MappingKey = {'platform_tracking_id'}
     */
    public final Map<String, SQLExpr> mappingKeyNameWithSQLExpr = new HashMap<>(2);
}
