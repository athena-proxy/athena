package me.ele.jarch.athena.worker.manager;

import com.github.mpjct.jmpjct.mysql.proto.ERR;
import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.netty.BatchSessionContext;
import me.ele.jarch.athena.netty.state.BatchQuitState;
import me.ele.jarch.athena.netty.state.SESSION_STATUS;
import me.ele.jarch.athena.server.async.AsyncErrorHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zhengchao on 2017/1/10.
 */
public class BatchDBErrorManager extends Manager {
    private static Logger logger = LoggerFactory.getLogger(BatchDBErrorManager.class);
    private BatchSessionContext batchSessionContext;
    private AsyncErrorHandler.ERR_TYPE errType;
    private ERR errPacket;
    private String localClientId;

    public BatchDBErrorManager(BatchSessionContext batchSessionContext,
        AsyncErrorHandler.ERR_TYPE reason, ERR errPacket, String localClientId) {
        super(BatchDBErrorManager.class.getName(), batchSessionContext.getSqlSessionContext());
        this.batchSessionContext = batchSessionContext;
        this.errType = reason;
        this.errPacket = errPacket;
        this.localClientId = localClientId;
    }

    @Override protected void manage() {
        if (!this.batchSessionContext.isAlive() || errType == null
            || errType == AsyncErrorHandler.ERR_TYPE.EMPTY) {
            return;
        }
        batchSessionContext.clientHasReturned(this.localClientId);
        SESSION_STATUS previousStatus = batchSessionContext.getCurrentState().getStatus();
        batchSessionContext.setState(SESSION_STATUS.BATCH_QUIT);
        BatchQuitState quitState = (BatchQuitState) batchSessionContext.getCurrentState();
        quitState.setPreviousStatus(previousStatus);
        quitState.setErrType(errType);
        quitState.setErrPacket(errPacket);
        quitState.setCurrLocalClientId(localClientId);
        try {
            batchSessionContext.execute();
        } catch (QuitException e) {
            logger.error("error when BatchDBErrorManager.manage()", e);
            quitState.reset();
            batchSessionContext.closeUserConn();
        }
    }
}
