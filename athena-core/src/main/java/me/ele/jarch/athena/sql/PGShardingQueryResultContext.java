package me.ele.jarch.athena.sql;

import me.ele.jarch.athena.allinone.DBVendor;
import me.ele.jarch.athena.pg.proto.PGResponseClassifier;
import me.ele.jarch.athena.util.proto.ResponseClassifier;

import java.util.Objects;

/**
 * Created by jinghao.wang on 17/7/17.
 */
public class PGShardingQueryResultContext extends ShardingQueryResultContext {

    @Override protected AbstractIntColumnWatcher createIntColumnWatcher() {
        return AbstractIntColumnWatcher.intColumnWatcherFactory(DBVendor.PG);
    }

    @Override protected ResponseClassifier newResponseClassifier() {
        return new PGResponseClassifier();
    }

    @Override public void addRowDescription(byte[] rowDescription) {
        count += rowDescription.length;
        if (Objects.nonNull(sqlCtx)) {
            sqlCtx.currentWriteToDalCounts += rowDescription.length;
        }
        columBufSize = getColumBufSize() + rowDescription.length;
        responseClassifier.addColumn(rowDescription);
        intColumnWatcher.startIntColumnAnalyze(sqlCtx, rowDescription);
    }

    @Override public void addCommandComplete(byte[] commandComplete) {
        count += commandComplete.length;
        if (Objects.nonNull(sqlCtx)) {
            sqlCtx.currentWriteToDalCounts += commandComplete.length;
        }
        //for INSERT/UPDATE/DELETE response duplicate
        shardingBuf.add(commandComplete);
        responseClassifier.setCommandComplete(commandComplete);
    }

    @Override public void addReadyForQuery(byte[] readyForQuery) {
        count += readyForQuery.length;
        if (Objects.nonNull(sqlCtx)) {
            sqlCtx.currentWriteToDalCounts += readyForQuery.length;
        }
        //for INSERT/UPDATE/DELETE response duplicate
        shardingBuf.add(readyForQuery);
        responseClassifier.setReadyForQuery(readyForQuery);
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

    @Override public void sendAndFlush2Client() {
        throw new RuntimeException("Called sendAndFlush2Client in PGShardingQueryResultContext");
    }
}
