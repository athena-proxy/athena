package me.ele.jarch.athena.worker.manager;

import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.netty.BatchSessionContext;
import me.ele.jarch.athena.netty.state.BatchResultState;
import me.ele.jarch.athena.sql.ResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zhengchao on 2017/1/9.
 */
public class BatchDBResultManager extends Manager {
    private static Logger logger = LoggerFactory.getLogger(BatchDBResultManager.class);
    private BatchSessionContext batchSessionContext;
    private ResultSet resultSet;
    private String localClientId;

    public BatchDBResultManager(BatchSessionContext batchSessionContext, ResultSet rs,
        String localClientId) {
        super(BatchDBResultManager.class.getName(), batchSessionContext.getSqlSessionContext());
        this.batchSessionContext = batchSessionContext;
        this.resultSet = rs;
        this.localClientId = localClientId;
    }

    @Override protected void manage() {
        if (!(batchSessionContext.getCurrentState() instanceof BatchResultState)) {
            return;
        }
        batchSessionContext.clientHasReturned(this.localClientId);
        BatchResultState state = (BatchResultState) batchSessionContext.getCurrentState();
        state.setResultSet(this.resultSet);
        try {
            batchSessionContext.execute();
        } catch (QuitException e) {
            logger.error("error when BatchDBResultManager.manage()", e);
            state.reset();
            batchSessionContext.closeUserConn();
        }
    }
}
