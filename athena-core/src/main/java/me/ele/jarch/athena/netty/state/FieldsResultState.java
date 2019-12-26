package me.ele.jarch.athena.netty.state;

import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.sql.CmdFieldsResult;
import me.ele.jarch.athena.sql.QueryResultContext;

public class FieldsResultState implements State {
    private SqlSessionContext sqlSessionContext;
    private volatile CmdFieldsResult fields = null;

    public FieldsResultState(SqlSessionContext sqlSessionContext) {
        this.sqlSessionContext = sqlSessionContext;
    }

    @Override public boolean handle() throws QuitException {
        return doFieldsResult();
    }

    @Override public SESSION_STATUS getStatus() {
        return SESSION_STATUS.QUERY_RESULT;
    }

    private boolean doFieldsResult() throws QuitException {
        if (fields == null) {
            QueryResultContext ctx = new QueryResultContext();
            ctx.packets = sqlSessionContext.dbServerPackets;
            ctx.sqlCtx = sqlSessionContext;
            fields = new CmdFieldsResult(ctx);
        } else {
            fields.getCtx().packets = sqlSessionContext.dbServerPackets;
        }
        fields.execute();
        if (fields.isDone()) {
            sqlSessionContext.sqlSessionContextUtil.resetQueryTimeRecord();
            try {
                // 结束etrace对dal连接数据库的跟踪事务
                sqlSessionContext.sqlSessionContextUtil.completeTransaction();
                // 结束etrace跟踪
                sqlSessionContext.sqlSessionContextUtil.endEtrace();
                // setStatus(SESSION_STATUS.QUERY_ANALYZE);
                sqlSessionContext.setState(SESSION_STATUS.QUERY_ANALYZE);
                sqlSessionContext.returnResource();
                fields.getCtx().sendAndFlush2Client();
            } finally {
                sqlSessionContext.releaseDbSemaphore();
            }
            fields = null;
        }
        return false;
    }

}
