package me.ele.jarch.athena.sharding.sql;

import me.ele.jarch.athena.sharding.ShardingTable;

import java.util.ArrayList;
import java.util.List;

public class SimpleIterableShardingResult extends IterableShardingResult {

    private List<ShardingResult> shardingResults = new ArrayList<ShardingResult>();

    public SimpleIterableShardingResult() {
        this(new ArrayList<ShardingResult>());
    }

    public SimpleIterableShardingResult(List<ShardingResult> shardingResults) {
        super(new ArrayList<ShardingTable>());
        this.shardingResults = shardingResults;
        shardingResults.forEach(t -> {
            tables.add(t.shardTable);
            databases.add(t.shardDB);
        });
        count = shardingResults.size();
    }

    @Override public boolean hasNext() {
        if (shardingTables == null) {
            return false;
        }
        return index < count;
    }

    @Override public ShardingResult next() {
        currentResult = shardingResults.get(index);
        index++;
        return currentResult;
    }
}
