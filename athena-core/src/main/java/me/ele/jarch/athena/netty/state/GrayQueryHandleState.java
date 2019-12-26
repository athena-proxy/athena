package me.ele.jarch.athena.netty.state;

import com.github.mpjct.jmpjct.util.ErrorCode;
import me.ele.jarch.athena.allinone.DBGroup;
import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.netty.SessionQuitTracer.QuitTrace;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.scheduler.Scheduler;
import me.ele.jarch.athena.server.pool.ResponseHandler;
import me.ele.jarch.athena.server.pool.ServerSession;
import me.ele.jarch.athena.sql.CmdQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrayQueryHandleState implements State {
    private static final Logger logger = LoggerFactory.getLogger(GrayQueryHandleState.class);
    final private SqlSessionContext sqlSessionContext;

    public GrayQueryHandleState(SqlSessionContext sqlSessionContext) {
        this.sqlSessionContext = sqlSessionContext;
    }

    @Override public boolean handle() throws QuitException {
        switchGrayState();
        return false;
    }

    @Override public SESSION_STATUS getStatus() {
        return SESSION_STATUS.GRAY_QUERY_HANDLE;
    }

    private void switchGrayState() throws QuitException {
        try {
            sqlSessionContext.setState(SESSION_STATUS.GRAY_RESULT);
            sendGray();
        } catch (Exception e) {
            logger.error("Exception caught in switchGrayState.", e);
            CmdQuery query = sqlSessionContext.curCmdQuery;
            if (query != null & query.isGrayRead()) {
                throw new QuitException("GrayRead Failed", e);
            }
            sqlSessionContext.grayUp.clearGrayUp();
        }
    }

    private void sendGray() throws QuitException {
        ServerSession session = sqlSessionContext.grayUp.serverSession;
        if (session == null) {
            sendGrayBuf(sqlSessionContext.grayUp.query.getSendBuf());
        } else {
            // 开启etrace对dal连接数据库的跟踪事务
            sqlSessionContext.sqlSessionContextUtil.startNewTransaction(session);
            sqlSessionContext.sqlSessionContextUtil.getGrayQueryStatistics()
                .setsSendTime(System.currentTimeMillis());
            session.dbServerWriteAndFlush(sqlSessionContext.grayUp.query.getSendBuf(), (s) -> {
                errorHandle(s);
            });
        }
    }

    /**
     * Asyn Create Session
     */
    private void sendGrayBuf(byte[] buf) throws QuitException {
        Scheduler sche = sqlSessionContext.scheduler;
        DBGroup dbGroup = sqlSessionContext.getHolder().getHomeDbGroup();
        dbGroup.acquireServerSession(sche.getInfo(), sqlSessionContext, new ResponseHandler() {

            @Override public void sessionAcquired(ServerSession session) {
                if (sqlSessionContext.attachGrayServerSession(session)) {
                    // 开启etrace对dal连接数据库的跟踪事务
                    sqlSessionContext.sqlSessionContextUtil.startNewTransaction(session);
                    sqlSessionContext.sqlSessionContextUtil.getGrayQueryStatistics()
                        .setsSendTime(System.currentTimeMillis());
                    session.dbServerWriteAndFlush(buf, (s) -> {
                        errorHandle(s);
                    });
                }
            }
        });
    }

    private void errorHandle(ServerSession s) {
        logger.error(
            "Error while sending to gray " + s.getActiveQulifiedDbId() + "," + sqlSessionContext);
        CmdQuery query = sqlSessionContext.curCmdQuery;
        if (query != null && query.isGrayRead()) {
            String message = "db connection abort";
            try {
                sqlSessionContext.quitTracer.reportQuit(QuitTrace.Send2ServerBroken);
                message = String.format("db connection abort [%s]", s.getActiveQulifiedDbId());
            } finally {
                sqlSessionContext.kill(ErrorCode.ABORT_SERVER_BROKEN, message);
            }
        } else {
            sqlSessionContext.grayUp.clearGrayUp();
        }
    }

}
