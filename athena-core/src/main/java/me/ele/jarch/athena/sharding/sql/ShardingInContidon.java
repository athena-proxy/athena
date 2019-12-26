package me.ele.jarch.athena.sharding.sql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLInListExpr;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * Created by jinghao.wang on 2017/11/5.
 */
class ShardingInContidon {
    String name = "";
    Optional<SQLInListExpr> shardingInAstNodeOp = Optional.empty();
    /**
     * 存放用于sharding列的值及其对应的语法节点。
     * 主要目的为了实现对SELECT ... IN场景的sharded sql
     * 重写in中的值。来避免对每个分片表无谓的扫描。
     */
    Map<String, SQLExpr> idWithSQLExpr = Collections.emptyMap();
}
