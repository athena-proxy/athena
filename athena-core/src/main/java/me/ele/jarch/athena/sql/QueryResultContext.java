package me.ele.jarch.athena.sql;

import com.github.mpjct.jmpjct.mysql.proto.ERR;
import com.github.mpjct.jmpjct.mysql.proto.OK;
import io.netty.buffer.Unpooled;
import me.ele.jarch.athena.allinone.DBVendor;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.server.pool.ServerSession;
import me.ele.jarch.athena.util.common.ReadOnlyBoolean;

import java.util.Queue;

public class QueryResultContext {
    public static final int SENT_BUFFER_SIZE = 64 * 1024;
    protected AbstractIntColumnWatcher intColumnWatcher = createIntColumnWatcher();

    protected AbstractIntColumnWatcher createIntColumnWatcher() {
        return AbstractIntColumnWatcher.intColumnWatcherFactory(DBVendor.MYSQL);
    }

    public Queue<byte[]> packets;

    public SqlSessionContext sqlCtx;
    protected int count = 0;
    protected int bufSize = 0;
    // queryResponse packets number start with 1.
    long sequenceId = 1;

    private ReadOnlyBoolean resultTypeLatch = new ReadOnlyBoolean(false);
    public ResultType resultType = ResultType.Other;

    public QueryResultContext() {
    }

    private boolean endTrans = false;
    public ERR err = null;
    public OK ok = null;

    public QueryResultContext(boolean endTrans) {
        this.endTrans = endTrans;
    }

    public boolean isEndTrans() {
        return endTrans;
    }

    public void writeBytes(byte[] src) {
        sequenceId++;
        count += src.length;
        sqlCtx.writeToClientCounts = count;
        sqlCtx.currentWriteToDalCounts = count;
        sqlCtx.clientLazyWrite(Unpooled.wrappedBuffer(src));
        bufSize += src.length;
        if (bufSize > SENT_BUFFER_SIZE) {
            //由于生产上大部分的sql结果集都小于64K,因此只要大于64K,就
            //启动autoread模式
            ServerSession serverSession =
                sqlCtx.shardedSession.get(sqlCtx.scheduler.getInfo().getGroup());
            sqlCtx.clientWriteAndFlushWithTrafficControll(Unpooled.wrappedBuffer(new byte[] {}),
                serverSession);
            bufSize = 0;
        }
    }

    /**
     * this method only allow call in PostgreSQL protocol
     *
     * @param rowDescription
     */
    public void addRowDescription(final byte[] rowDescription) {
        writeBytes(rowDescription);
        intColumnWatcher.startIntColumnAnalyze(sqlCtx, rowDescription);
    }

    /**
     * this method only allow call in PostgreSQL protocol
     *
     * @param commandComplete
     */
    public void addCommandComplete(final byte[] commandComplete) {
        writeBytes(commandComplete);
    }

    /**
     * this method only allow call in PostgreSQL protocol
     *
     * @param readyForQuery
     */
    public void addReadyForQuery(final byte[] readyForQuery) {
        writeBytes(readyForQuery);
    }

    /**
     * this method only allow call in MySQL protocol
     *
     * @param column
     */
    public void addColumn(byte[] column) {
        writeBytes(column);
        intColumnWatcher.startIntColumnAnalyze(sqlCtx, column);
    }

    public void setResultType(ResultType type) {
        if (resultTypeLatch.compareAndSet(false, true)) {
            this.resultType = type;
        }
    }

    public void addRow(byte[] row) {
        writeBytes(row);
        intColumnWatcher.fireTraceIfNecessary(row);
    }

    public void sendAndFlush2Client() {
        count = 0;
        bufSize = 0;
        sqlCtx.clientWriteAndFlush(Unpooled.EMPTY_BUFFER);
    }
}
