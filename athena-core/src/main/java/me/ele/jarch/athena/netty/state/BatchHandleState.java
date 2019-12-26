package me.ele.jarch.athena.netty.state;

import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.netty.BatchSessionContext;
import me.ele.jarch.athena.server.async.AsyncLocalClient;
import me.ele.jarch.athena.sql.BatchQuery;
import me.ele.jarch.athena.sql.QUERY_TYPE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

/**
 * Created by zhengchao on 16/8/29.
 */
public class BatchHandleState implements State {
    private static final Logger logger = LoggerFactory.getLogger(BatchHandleState.class);
    private BatchSessionContext batchContext;

    public BatchHandleState(BatchSessionContext batchContext) {
        this.batchContext = Objects.requireNonNull(batchContext,
            () -> "BatchContext must be not null, msg: " + toString());
    }

    @Override public boolean handle() throws QuitException {
        sendBatchQuery();
        batchContext.setState(SESSION_STATUS.BATCH_RESULT);
        return false;
    }

    private void sendBatchQuery() {
        for (Entry<String, BatchQuery> entry : batchContext.getQuerySqls().entrySet()) {
            if (!sendBatchQuery(entry.getValue())) {
                return;
            }
        }
    }

    private boolean sendBatchQuery(BatchQuery batchQuery) {
        Map<String, AsyncLocalClient> clients = batchContext.clients;
        if (!send2AllClients(batchQuery.getType())) {
            AsyncLocalClient client = clients.computeIfAbsent(batchQuery.getShardTable(),
                shardTable -> new AsyncLocalClient(batchContext.generateLocalClientId()));
            return sendBatchQueryOrCloseUserConn(client, batchQuery);
        }
        // deal commit,rollback
        for (Entry<String, AsyncLocalClient> entry : clients.entrySet()) {
            if (!sendBatchQueryOrCloseUserConn(entry.getValue(), batchQuery)) {
                return false;
            }
        }
        return true;
    }

    private boolean sendBatchQueryOrCloseUserConn(AsyncLocalClient client, BatchQuery batchQuery) {
        if (client.isAlive()) {
            return batchContext.sendBatchQuery(client, batchQuery);
        }
        logger.error("{},batchClientId={},detect that the client has been dead.",
            batchContext.batchTransLogIdStr, client.getId());
        batchContext.closeUserConn();
        return false;
    }

    private boolean send2AllClients(QUERY_TYPE type) {
        return type.isEndTrans();
    }

    @Override public SESSION_STATUS getStatus() {
        return SESSION_STATUS.BATCH_HANDLE;
    }

    @Override public String toString() {
        return "BatchHandleState";
    }

}
