package me.ele.jarch.athena.netty.state;

import io.netty.buffer.Unpooled;
import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.sql.CmdQuery;
import me.ele.jarch.athena.sql.CmdQueryResult;
import me.ele.jarch.athena.sql.GrayQueryResultContext;
import me.ele.jarch.athena.sql.QueryResultContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class GrayState implements State {

    private static final Logger logger = LoggerFactory.getLogger(GrayState.class);
    private volatile CmdQueryResult result = null;
    private final SqlSessionContext sqlSessionContext;

    public GrayState(SqlSessionContext sqlSessionContext) {
        this.sqlSessionContext = sqlSessionContext;
    }

    @Override public boolean handle() throws QuitException {
        doGrayUp();
        return false;
    }

    @Override public SESSION_STATUS getStatus() {
        return SESSION_STATUS.GRAY_RESULT;
    }

    private void doGrayUp() throws QuitException {
        sqlSessionContext.queryId = -1;
        try {
            if (result == null) {
                QueryResultContext ctx = new GrayQueryResultContext();
                if (sqlSessionContext.curCmdQuery.isGrayRead()) {
                    ctx = new QueryResultContext();
                }
                ctx.sqlCtx = sqlSessionContext;
                ctx.packets = sqlSessionContext.dbServerPackets;
                result = new CmdQueryResult(ctx);
            } else {
                result.getCtx().packets = sqlSessionContext.dbServerPackets;
            }
            result.execute();
            if (result.isDone()) {
                try {
                    sqlSessionContext.sqlSessionContextUtil.doGrayStatistics();
                    sqlSessionContext.setState(SESSION_STATUS.QUERY_ANALYZE);
                    sqlSessionContext.grayUp.returnResource();
                    sqlSessionContext.curCmdQuery = null;
                    if (sqlSessionContext.grayUp.query.queryType.isEndTrans()) {
                        sqlSessionContext.grayUp.setStarted(false);
                    }
                    // 结束etrace对dal连接数据库的跟踪事务
                    sqlSessionContext.sqlSessionContextUtil.completeTransaction();
                    // 结束etrace跟踪
                    sqlSessionContext.sqlSessionContextUtil.endEtrace();
                    sqlSessionContext.clientWriteAndFlush(Unpooled.EMPTY_BUFFER);
                } finally {
                    result = null;
                    sqlSessionContext.grayUp.releaseDbSemaphore();
                }
            }
        } catch (Exception e) {
            logger.error(Objects.toString(e));
            CmdQuery query = sqlSessionContext.curCmdQuery;
            if (query != null & query.isGrayRead()) {
                throw new QuitException("GrayRead Failed", e);
            }
            sqlSessionContext.grayUp.clearGrayUp();
        }
    }
}
