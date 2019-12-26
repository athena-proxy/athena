package me.ele.jarch.athena.netty.state;

import com.github.mpjct.jmpjct.mysql.proto.ERR;
import com.github.mpjct.jmpjct.util.ErrorCode;
import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.netty.BatchSessionContext;
import me.ele.jarch.athena.server.async.AbstractAsyncErrorHandler;
import me.ele.jarch.athena.server.async.AsyncLocalClient;
import me.ele.jarch.athena.sql.BatchQuery;
import me.ele.jarch.athena.worker.SchedulerWorker;
import me.ele.jarch.athena.worker.manager.BatchDBErrorManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by zhengchao on 16/8/31.
 */
public class BatchQuitState extends AbstractAsyncErrorHandler implements State {
    private static final Logger logger = LoggerFactory.getLogger(BatchQuitState.class);
    private static final int CLINET_CONNECT_FAIL_MAX_RETRY_TIMES = 10000;
    private int currClientConnectFailRetry = 0;
    private final BatchSessionContext batchContext;
    private ERR_TYPE reason = ERR_TYPE.EMPTY;
    private ERR errPacket = null;
    private String currLocalClientId = "";
    private final AtomicBoolean normalQuit = new AtomicBoolean(false);
    private SESSION_STATUS prevStatus = null;

    public BatchQuitState(BatchSessionContext batchContext) {
        this.batchContext = Objects.requireNonNull(batchContext,
            () -> "BatchContext must be not null, msg: " + toString());
    }

    @Override public boolean handle() throws QuitException {
        boolean handleDone = false;
        switch (reason) {
            case EMPTY:
                dealClientNormalQuit();
                handleDone = true;
                break;
            case QUERY_FAILED:
                dealClientQueryFailed();
                break;
            case CONNECT_FAILED:
                dealClientConnectFailed();
            case CHANNEL_INACTIVE:
                dealClientInactive();
                break;
            default:
                logger.error("invalid type for AsyncErrorHandler.ERR_TYPE:{}", reason);
        }
        reset();
        return handleDone;
    }

    private void dealClientNormalQuit() {
        if (!this.batchContext.isAlive()) {
            return;
        }
        this.normalQuit.set(true);
        batchContext.markAsDead();
        batchContext.clients.forEach((shardTable, client) -> client.doQuit("", false));
        batchContext.clients.clear();
        batchContext.reset();
    }

    private void dealClientQueryFailed() {
        resume2PrevStatus();
        if (errPacket == null) {
            batchContext.trySendBack2UserOrCloseUser();
            return;
        }
        String loggedErrMsg = String
            .format("%s,batchClientId=%s,error happens", batchContext.batchTransLogIdStr,
                this.currLocalClientId);
        if (BatchSessionContext.isAbortingErrorCode(errPacket.errorCode)) {
            batchContext.asyncClientAborted(errPacket);
            loggedErrMsg = String.format("%s:ER_ABORTING_CONNECTION", loggedErrMsg);
        }
        batchContext.addErrorPacketIfNonAbortingDetected(errPacket);
        logger.error(loggedErrMsg);

        batchContext.trySendBack2UserOrCloseUser();
    }

    private void dealClientConnectFailed() {
        resume2PrevStatus();
        AsyncLocalClient oldClient =
            batchContext.queryUsedClients.getOrDefault(currLocalClientId, null);
        if (Objects.isNull(oldClient)) {
            logger.error("{},batchClientId={},strange,illegal localClientId to try to reconnect",
                batchContext.batchTransLogIdStr, currLocalClientId);
            return;
        }
        if (currClientConnectFailRetry >= CLINET_CONNECT_FAIL_MAX_RETRY_TIMES) {
            logger.error(
                "{},local client connects still failure after retrying {} times,currentBatchClientId={}",
                batchContext.batchTransLogIdStr, currClientConnectFailRetry, currLocalClientId);
            batchContext.closeUserConn();
            return;
        }
        currClientConnectFailRetry++;
        logger.error("{},the {}th time to reconnect local channel server,currentBatchClientId={}",
            batchContext.batchTransLogIdStr, currClientConnectFailRetry, currLocalClientId);
        AsyncLocalClient newClient = new AsyncLocalClient(currLocalClientId);
        BatchQuery batchQuery = oldClient.getBatchQuery();
        batchContext.clients.put(batchQuery.getShardTable(), newClient);
        batchContext.sendBatchQuery(newClient, batchQuery);
    }

    private void dealClientInactive() {
        resume2PrevStatus();
        if (!this.normalQuit.get()) {
            batchContext.asyncClientAborted(
                ERR.buildErr(1, ErrorCode.ER_ABORTING_CONNECTION.getErrorNo(),
                    "AsyncLocalClient channel inactive"));
        }
        batchContext.trySendBack2UserOrCloseUser();
    }

    public void setPreviousStatus(SESSION_STATUS status) {
        this.prevStatus = status;
    }

    public void setErrType(ERR_TYPE errType) {
        this.reason = errType;
    }

    public void setErrPacket(ERR errPacket) {
        this.errPacket = errPacket;
    }

    public void setCurrLocalClientId(String currLocalClientId) {
        this.currLocalClientId = currLocalClientId;
    }

    private void resume2PrevStatus() {
        // because the clients is concurrent,we must resume the state to avoid to influence other clients
        if (Objects.nonNull(prevStatus)) {
            batchContext.setState(prevStatus);
        }
    }

    public void reset() {
        this.reason = ERR_TYPE.EMPTY;
        this.errPacket = null;
    }

    @Override public void handleError(ERR_TYPE reason, ERR errPacket, String localClientId) {
        if (reason == null || reason == ERR_TYPE.EMPTY) {
            logger.error("{},batchClientId={},err reason is null or empty",
                batchContext.batchTransLogIdStr, localClientId);
            return;
        }
        if (!this.batchContext.isAlive()) {
            logger
                .info("{},batchClientId={},batchCtx has been dead", batchContext.batchTransLogIdStr,
                    localClientId);
            return;
        }
        SchedulerWorker.getInstance()
            .enqueue(new BatchDBErrorManager(batchContext, reason, errPacket, localClientId));
    }

    @Override public SESSION_STATUS getStatus() {
        return SESSION_STATUS.BATCH_QUIT;
    }

    @Override public String toString() {
        return "BatchQuitState";
    }
}
