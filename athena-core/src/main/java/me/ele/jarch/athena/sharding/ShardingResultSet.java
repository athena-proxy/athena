package me.ele.jarch.athena.sharding;

import me.ele.jarch.athena.util.AggregateFunc;

import java.util.ArrayList;
import java.util.List;

public abstract class ShardingResultSet {

    public long sequenceId = 1;
    public List<byte[]> columns = new ArrayList<>();
    public List<byte[]> rows = new ArrayList<>();
    public List<byte[]> commandCompletes = new ArrayList<>();
    public List<byte[]> readyForQuerys = new ArrayList<>();
    private int firstShardingIndex = -1;
    protected List<AggregateFunc> columnAggeTypes = new ArrayList<>();
    protected boolean isAggrMethods = false;
    protected boolean isGroupBy = false;

    public ShardingResultSet(List<AggregateFunc> columnAggeTypes, boolean isGroupBy) {
        this.columnAggeTypes = columnAggeTypes;
        this.isAggrMethods = columnAggeTypes.stream().anyMatch(
            method -> method.equals(AggregateFunc.COUNT) || method.equals(AggregateFunc.SUM)
                || method.equals(AggregateFunc.MAX) || method.equals(AggregateFunc.MIN));
        this.isGroupBy = isGroupBy;
    }

    /**
     * 对于MySQL协议,列定义是多个协议包传输的,可能存在总共10个列定义,分多次到达
     * DAL的情况。所以需要保证列定义的接收要收全，且只收第一个sharding index的列定义。
     * 对于PostgreSQL协议,列定义是由一个协议包传输的,只需要收第一个sharding index的
     * 列定义一次即可,后续列定义都忽略，其行为与MySQL协议兼容。
     *
     * @param columns
     * @param shardingIndex
     */
    public void addColumn(List<byte[]> columns, int shardingIndex) {
        if (setShardingIndex(shardingIndex)) {
            this.columns.addAll(columns);
            return;
        }
    }

    public void addRow(List<byte[]> rows, int shardingIndex) {
        if (setShardingIndex(shardingIndex)) {
            this.rows.addAll(rows);
            return;
        }
        if (isAggregatable()) {
            aggregateRows(rows);
            return;
        }
        this.rows.addAll(rows);
    }

    /**
     * Only called in PostgreSQL protocol
     *
     * @param commandComplete
     */
    public void addCommandComplete(byte[] commandComplete) {
        commandCompletes.add(commandComplete);
    }

    /**
     * Only called in PostgreSQL protocol
     *
     * @param readForQuery
     */
    public void addReadyForQuery(byte[] readForQuery) {
        readyForQuerys.add(readForQuery);
    }

    protected boolean isAggregatable() {
        return isAggrMethods && !isGroupBy;
    }

    /**
     * @param shardingIndex
     * @return return true when the shardingIndex is the first
     */
    boolean setShardingIndex(int shardingIndex) {
        if (this.firstShardingIndex == shardingIndex) {
            return true;
        }
        if (this.firstShardingIndex == -1) {
            this.firstShardingIndex = shardingIndex;
            return true;
        }
        return false;
    }

    public abstract List<byte[]> toPartPackets(boolean withEOF);

    protected abstract void aggregateRows(List<byte[]> newRows);

    public int getSelectResultSetBufSize() {
        if (isAggregatable()) {
            return 0;
        }
        int result = 0;
        for (byte[] row : rows) {
            result += row.length;
        }
        for (byte[] col : columns) {
            result += col.length;
        }
        return result;
    }
}
