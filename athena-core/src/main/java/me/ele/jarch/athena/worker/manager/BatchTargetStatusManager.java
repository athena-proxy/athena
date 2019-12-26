package me.ele.jarch.athena.worker.manager;

import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.netty.BatchSessionContext;
import me.ele.jarch.athena.netty.state.SESSION_STATUS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zhengchao on 2017/1/10.
 */
public class BatchTargetStatusManager extends Manager {
    private static Logger logger = LoggerFactory.getLogger(BatchTargetStatusManager.class);
    private BatchSessionContext batchSessionContext;
    private SESSION_STATUS targetStatus;

    public BatchTargetStatusManager(BatchSessionContext batchSessionContext,
        SESSION_STATUS targetStatus) {
        super(BatchTargetStatusManager.class.getName(), batchSessionContext.getSqlSessionContext());
        this.batchSessionContext = batchSessionContext;
        this.targetStatus = targetStatus;
    }

    @Override protected void manage() {
        try {
            batchSessionContext.setState(targetStatus);
            batchSessionContext.execute();
        } catch (QuitException e) {
            logger.error("error when BatchTargetStatusManager.manage()", e);
            this.batchSessionContext.closeUserConn();
        }
    }
}
