package me.ele.jarch.athena.sharding.sql;

import me.ele.jarch.athena.sharding.ShardingRouter;
import me.ele.jarch.athena.sharding.ShardingTable;
import me.ele.jarch.athena.util.AggregateFunc;

import java.util.*;

public class IterableShardingResult implements Iterator<ShardingResult> {
    public final static IterableShardingResult EMPTY_ITERABLE_SHARDING_RESULT =
        new IterableShardingResult(null);

    public int count = 0;
    public List<String> tables = new ArrayList<>();
    public List<String> databases = new ArrayList<>();
    public ShardingResult currentResult = null;

    // column aggr method name
    public List<AggregateFunc> aggrMethods = new ArrayList<>();

    public List<String> groupByItems = new ArrayList<>();

    protected List<ShardingTable> shardingTables;
    protected int index = 0;
    protected int shardingRuleCount = -1;

    /**
     * origin name and values by sharding
     */
    public Map<String, Set<String>> originShardingKey = Collections.emptyMap();

    public IterableShardingResult(List<ShardingTable> shardingTables, int shardingRuleCount) {
        if (shardingTables == null) {
            return;
        }
        this.shardingTables = shardingTables;
        shardingTables.forEach(t -> {
            tables.add(t.table);
            databases.add(t.database);
        });
        count = shardingTables.size();
        this.shardingRuleCount = shardingRuleCount;
    }

    public IterableShardingResult(List<ShardingTable> shardingTables) {
        this(shardingTables, -1);
    }

    @Override public boolean hasNext() {
        if (shardingTables == null) {
            return false;
        }
        return index < shardingTables.size();
    }

    @Override public ShardingResult next() {
        throw new NoSuchElementException();
    }

    //TODO 需要高效精确的方法来判定是否是全表sharding
    final public boolean isAllSharding(ShardingRouter router, String table) {
        if (table == null || router == null) {
            return false;
        }
        if (!router.isShardingTable(table)) {
            return false;
        }
        List<ShardingTable> list = router.getAllShardingTable(table);
        if (list != null && list.size() == tables.size()) {
            return true;
        }
        return false;
    }

    public int getIndex() {
        return index;
    }
}
