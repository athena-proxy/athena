package me.ele.jarch.athena.server.pool;

import com.github.mpjct.jmpjct.mysql.proto.OK;
import io.netty.channel.embedded.EmbeddedChannel;
import me.ele.jarch.athena.allinone.DBConnectionInfo;
import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.worker.manager.BatchContextManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Objects;
import java.util.Queue;

/**
 * @author shaoyang.qi
 */
public class BatchOperateServerSession extends ServerSession {
    private static Logger logger = LoggerFactory.getLogger(BatchOperateServerSession.class);
    private BatchContextManager batchContextManager = null;

    public BatchOperateServerSession(DBConnectionInfo info, ServerSessionPool pool) {
        super(info, pool);
        skip2OKStatus();
        serverChannel = new EmbeddedChannel(EMPTY_HANDLER);
    }

    private void skip2OKStatus() {
        Queue<byte[]> queue = new LinkedList<>();
        queue.add(OK.getOkPacketBytes());
        setServerPackets(queue);
        setStatus(SERVER_SESSION_STATUS.RECEIVE_LAST_OK);
    }

    private BatchContextManager getBatchContextManager(SqlSessionContext sqlSessionContext) {
        if (this.batchContextManager == null) {
            this.batchContextManager = new BatchContextManager(this, sqlSessionContext);
        }
        return this.batchContextManager;
    }

    @Override public synchronized void doQuit() {
        super.doQuit();
        reset();
    }

    @Override public void closeServerSession() {
        super.closeServerSession();
        reset();
    }

    @Override public void returnServerSession() throws QuitException {
        reset();
        super.returnServerSession();
    }

    private void reset() {
        if (this.batchContextManager == null) {
            return;
        }
        this.batchContextManager.release();
        this.batchContextManager = null;
    }

    @Override
    public void dbServerWriteAndFlush(byte[] inputBuf, ServerSessionErrorHandler handler) {
        if (inputBuf == null || inputBuf.length <= 0) {
            return;
        }
        SqlSessionContext sqlSessionContext = sqlServerPackedDecoder.getSqlCtx();
        if (sqlSessionContext != null) {
            getBatchContextManager(sqlSessionContext).setPacketAndEnqueue(inputBuf, handler);
        } else {
            logger.error("{} do not have a valid sqlctx", toString());
        }
    }

    public String toString() {
        SqlSessionContext sqlCtx = sqlServerPackedDecoder.getSqlCtx();
        String isAutoCommit4Client = Objects.isNull(sqlCtx) ?
            "unknown" :
            String.valueOf(sqlCtx.transactionController.isAutoCommit());
        return String
            .format("BatchOperateServerSession{connId=%s, dbConnInfo=%s, isAutoCommit4Client=%s}",
                getConnectionId(), activeQulifiedDbId, isAutoCommit4Client);
    }
}
