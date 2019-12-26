package me.ele.jarch.athena.netty.state;

import com.github.mpjct.jmpjct.mysql.proto.Flags;
import me.ele.jarch.athena.allinone.DBConnectionInfo;
import me.ele.jarch.athena.allinone.DBGroup;
import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.server.pool.ResponseHandler;
import me.ele.jarch.athena.server.pool.ServerSession;
import me.ele.jarch.athena.sharding.sql.IterableShardingResult;
import me.ele.jarch.athena.sharding.sql.ShardingResult;
import me.ele.jarch.athena.sql.CmdQuery;

import java.util.concurrent.atomic.AtomicLong;

public class QueryHandleState implements State {
    protected final SqlSessionContext sqlSessionContext;
    protected static AtomicLong queryIdGen = new AtomicLong();
    private static AtomicLong secondCommitIdGen = new AtomicLong();

    public QueryHandleState(SqlSessionContext sqlSessionContext) {
        this.sqlSessionContext = sqlSessionContext;
    }

    @Override public boolean handle() throws QuitException {
        return doHandleQuery();
    }

    @Override public SESSION_STATUS getStatus() {
        return SESSION_STATUS.QUERY_HANDLE;
    }

    // used for record when sharding, send first commit, not send second commit
    private void addFirstCommit() {
        SqlSessionContext sqlCtx = sqlSessionContext;
        if (!sqlCtx.isInTransStatus()) {
            return;
        }
        if (sqlCtx.shardingCount != 2) {
            return;
        }
        if (!sqlCtx.getQueryType().isEndTrans()) {
            return;
        }
        IterableShardingResult isr = sqlCtx.shardingResult;
        if (isr == null) {
            return;
        }
        ShardingResult sr = isr.currentResult;
        if (sr == null) {
            return;
        }
        if (sr.shardingRuleIndex == 0) {
            sqlCtx.sqlSessionContextUtil.secondCommitId =
                secondCommitIdGen.incrementAndGet() * 2 + 1;
        }
    }

    protected boolean doHandleQuery() throws QuitException {
        CmdQuery query = sqlSessionContext.curCmdQuery;
        if (query == null) {
            throw new QuitException("strange! CmdQuery query is null in Session");
        }

        if (isMysqlComFieldList(query.type)) {
            sqlSessionContext.setState(SESSION_STATUS.FIELDS_RESULT);
        } else {
            sqlSessionContext.setState(SESSION_STATUS.QUERY_RESULT);
        }
        addFirstCommit();
        ServerSession serverSession =
            sqlSessionContext.shardedSession.get(sqlSessionContext.scheduler.getInfo().getGroup());
        if (serverSession == null) {
            sendBuf(assembleFirstSendBuf(query));
        } else {
            sqlSessionContext.sqlSessionContextUtil.getCurrentQueryStatistics()
                .setsSendTime(System.currentTimeMillis());
            // 开启etrace对dal连接数据库的跟踪事务
            sqlSessionContext.sqlSessionContextUtil.startNewTransaction(serverSession);
            serverSession
                .dbServerWriteAndFlush(query.getSendBuf(), sqlSessionContext.defaultErrorHandler);
        }
        sqlSessionContext.queryId = queryIdGen.incrementAndGet();
        return false;
    }

    protected boolean isMysqlComFieldList(byte type) {
        return type == Flags.COM_FIELD_LIST;
    }

    protected byte[] assembleFirstSendBuf(CmdQuery query) {
        return query.getSendBuf();
    }

    /**
     * Asyn Create Session
     */
    protected void sendBuf(byte[] buf) throws QuitException {
        String dbGroupName = sqlSessionContext.scheduler.getInfo().getGroup();
        DBGroup dbGroup = dbGroupName == null ?
            sqlSessionContext.getHolder().getHomeDbGroup() :
            sqlSessionContext.getHolder().getDbGroup(dbGroupName);
        DBConnectionInfo targetDb = sqlSessionContext.scheduler.getInfo();
        long dbAcquireStarttime = System.currentTimeMillis();
        dbGroup.acquireServerSession(targetDb, sqlSessionContext, new ResponseHandler() {

            @Override public void sessionAcquired(ServerSession session) {
                if (sqlSessionContext.attachServerSession(session)) {
                    sqlSessionContext.sqlSessionContextUtil.getCurrentQueryStatistics()
                        .setsSendTime(System.currentTimeMillis());
                    // 开启etrace对dal连接数据库的跟踪事务
                    startDBEtraceTransaction(session);
                    sqlSessionContext.sqlSessionContextUtil
                        .appendAwaitDbConnTime(System.currentTimeMillis() - dbAcquireStarttime);
                    session.dbServerWriteAndFlush(buf, sqlSessionContext.defaultErrorHandler);
                }
            }
        });
    }

    protected void startDBEtraceTransaction(ServerSession session) {
        sqlSessionContext.sqlSessionContextUtil.startNewTransaction(session);
    }
}
