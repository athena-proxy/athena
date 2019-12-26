package me.ele.jarch.athena.sql;

import com.github.mpjct.jmpjct.mysql.proto.MysqlResponseClassifier;
import me.ele.jarch.athena.util.proto.ResponseClassifier;

import java.util.ArrayList;

public class ShardingQueryResultContext extends QueryResultContext {

    public ArrayList<byte[]> shardingBuf = new ArrayList<>();
    public final ResponseClassifier responseClassifier;
    protected int rowBufSize = 0;
    protected int columBufSize = 0;
    private int ShardingIndex = -1;

    public ShardingQueryResultContext() {
        responseClassifier = newResponseClassifier();
    }

    protected ResponseClassifier newResponseClassifier() {
        return new MysqlResponseClassifier();
    }

    @Override public void writeBytes(byte[] src) {
        count += src.length;
        if (sqlCtx != null) {
            sqlCtx.currentWriteToDalCounts += src.length;
        }
        shardingBuf.add(src);
    }

    @Override public void addColumn(byte[] column) {
        count += column.length;
        if (sqlCtx != null) {
            sqlCtx.currentWriteToDalCounts += column.length;
        }
        columBufSize = getColumBufSize() + column.length;
        responseClassifier.addColumn(column);

        intColumnWatcher.startIntColumnAnalyze(sqlCtx, column);
    }

    @Override public void addRow(byte[] row) {
        count += row.length;
        if (sqlCtx != null) {
            sqlCtx.currentWriteToDalCounts += row.length;
        }
        rowBufSize = getRowBufSize() + row.length;
        responseClassifier.addRow(row);

        intColumnWatcher.fireTraceIfNecessary(row);
    }

    @Override public void addRowDescription(byte[] rowDescription) {
        throw new RuntimeException("Called addRowDescription in ShardingQueryResultContext");
    }

    @Override public void addCommandComplete(byte[] commandComplete) {
        throw new RuntimeException("Called addCommandComplete in ShardingQueryResultContext");
    }

    @Override public void addReadyForQuery(byte[] readyForQuery) {
        throw new RuntimeException("Called addReadyForQuery in ShardingQueryResultContext");
    }

    @Override public void sendAndFlush2Client() {
        throw new RuntimeException("Called sendAndFlush2Client in ShardingQueryResultContext");
    }

    public int getRowBufSize() {
        return rowBufSize;
    }

    public int getColumBufSize() {
        return columBufSize;
    }

    public void resetColumnBuf() {
        count -= columBufSize;
        columBufSize = 0;
        responseClassifier.columns.clear();
    }

    public void resetRowBuf() {
        count -= rowBufSize;
        rowBufSize = 0;
        responseClassifier.rows.clear();
    }

    public int getShardingIndex() {
        return ShardingIndex;
    }

    public void setShardingIndex(int shardingIndex) {
        ShardingIndex = shardingIndex;
    }

}
