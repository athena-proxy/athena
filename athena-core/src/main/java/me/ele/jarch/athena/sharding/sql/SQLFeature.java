package me.ele.jarch.athena.sharding.sql;

import me.ele.jarch.athena.allinone.DBVendor;

/**
 * 记录SQL语法分析捕捉到的特征
 * Created by jinghao.wang on 2018/3/7.
 */
public class SQLFeature {
    /* SQL 只有一个where条件, 且条件为 k = v 样式时为true, 其它为false */
    private boolean oneKey = false;
    /* SQL 是否有group by 子句 */
    private boolean groupBy = false;
    /* SQL 是否有order by 子句 */
    private boolean orderBy = false;
    /* SQL 是否有where条件 */
    private boolean where = false;
    /* SQL 只有一个where条件, 且条件为 k in (v1, v2, ..., vn) 样式时, value的个数大于0，其它都为-1 */
    private long singleInValues = -1;
    /* SQL limit的值, 如果有limit的话 */
    private long limit = -1;
    /* SQL offset的值, 如果有offset的话 */
    private long offset = -1;
    /* SQL 是单表查询时的表名, join, 子查询, 不带表名的SQL table 都为空值 */
    private String table = "";
    /* SQL 是否应该被Sharding*/
    private boolean needSharding = false;
    /* SQL 是否扫描了所有的sharding 分片*/
    private boolean shardingSelectAll = false;
    /* SQL 是MySQL协议还是PostgreSQL协议 */
    public final DBVendor vendor;

    public SQLFeature(DBVendor vendor) {
        this.vendor = vendor;
    }

    public boolean isOneKey() {
        return oneKey;
    }

    public void setOneKey(boolean oneKey) {
        this.oneKey = oneKey;
    }

    public boolean hasGroupBy() {
        return groupBy;
    }

    public void setGroupBy(boolean groupBy) {
        this.groupBy = groupBy;
    }

    public boolean hasOrderBy() {
        return orderBy;
    }

    public void setOrderBy(boolean orderBy) {
        this.orderBy = orderBy;
    }

    public boolean hasWhere() {
        return where;
    }

    public void setWhere(boolean where) {
        this.where = where;
    }

    public long getSingleInValues() {
        return singleInValues;
    }

    public void setSingleInValues(long singleInValues) {
        this.singleInValues = singleInValues;
    }

    public long getLimit() {
        return limit;
    }

    public void setLimit(long limit) {
        this.limit = limit;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public boolean isNeedSharding() {
        return needSharding;
    }

    public void setNeedSharding(boolean needSharding) {
        this.needSharding = needSharding;
    }

    public boolean isShardingSelectAll() {
        return shardingSelectAll;
    }

    public void setShardingSelectAll(boolean shardingSelectAll) {
        this.shardingSelectAll = shardingSelectAll;
    }
}
