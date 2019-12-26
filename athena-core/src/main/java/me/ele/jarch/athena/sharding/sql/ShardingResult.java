package me.ele.jarch.athena.sharding.sql;

import me.ele.jarch.athena.sql.QUERY_TYPE;

public class ShardingResult {
    // sharding output sql
    public String shardedSQL;

    // the table name of sharded sql
    public String shardTable;

    // the db of sharded sql
    public String shardDB;

    // select limit & offset
    public int limit = -1;
    public int offset = -1;

    // the id for insert,delete and update
    public String id = "";

    /**
     * 多维sharding时，用于标识关联的sharding维数
     */
    public int shardingRuleIndex = -1;
    /**
     * sharding维数的个数
     */
    public int shardingRuleCount = -1;

    public QUERY_TYPE queryType = QUERY_TYPE.OTHER;

    public ShardingResult(String shardedSQL, String shardTable, String shardDB) {
        this.shardedSQL = shardedSQL;
        this.shardTable = shardTable;
        this.shardDB = shardDB;
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        ShardingResult that = (ShardingResult) o;

        if (limit != that.limit)
            return false;
        if (offset != that.offset)
            return false;
        if (shardingRuleIndex != that.shardingRuleIndex)
            return false;
        if (shardingRuleCount != that.shardingRuleCount)
            return false;
        if (shardedSQL != null ? !shardedSQL.equals(that.shardedSQL) : that.shardedSQL != null)
            return false;
        if (shardTable != null ? !shardTable.equals(that.shardTable) : that.shardTable != null)
            return false;
        if (shardDB != null ? !shardDB.equals(that.shardDB) : that.shardDB != null)
            return false;
        if (id != null ? !id.equals(that.id) : that.id != null)
            return false;
        return queryType == that.queryType;
    }

    @Override public int hashCode() {
        int result = shardedSQL != null ? shardedSQL.hashCode() : 0;
        result = 31 * result + (shardTable != null ? shardTable.hashCode() : 0);
        result = 31 * result + (shardDB != null ? shardDB.hashCode() : 0);
        result = 31 * result + limit;
        result = 31 * result + offset;
        result = 31 * result + (id != null ? id.hashCode() : 0);
        result = 31 * result + shardingRuleIndex;
        result = 31 * result + shardingRuleCount;
        result = 31 * result + (queryType != null ? queryType.hashCode() : 0);
        return result;
    }

    @Override public String toString() {
        final StringBuilder sb = new StringBuilder("ShardingResult{");
        sb.append("shardedSQL='").append(shardedSQL).append('\'');
        sb.append(", shardTable='").append(shardTable).append('\'');
        sb.append(", shardDB='").append(shardDB).append('\'');
        sb.append(", limit=").append(limit);
        sb.append(", offset=").append(offset);
        sb.append(", id='").append(id).append('\'');
        sb.append(", shardingRuleIndex=").append(shardingRuleIndex);
        sb.append(", shardingRuleCount=").append(shardingRuleCount);
        sb.append(", queryType=").append(queryType);
        sb.append('}');
        return sb.toString();
    }
}
