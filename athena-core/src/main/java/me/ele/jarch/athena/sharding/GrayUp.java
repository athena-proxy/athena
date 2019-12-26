package me.ele.jarch.athena.sharding;

import io.netty.buffer.Unpooled;
import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.netty.state.SESSION_STATUS;
import me.ele.jarch.athena.scheduler.EmptyScheduler;
import me.ele.jarch.athena.server.pool.ServerSession;
import me.ele.jarch.athena.sql.CmdQuery;
import me.ele.jarch.athena.sql.QUERY_TYPE;
import me.ele.jarch.athena.util.AthenaConfig;
import me.ele.jarch.athena.util.GrayType;
import me.ele.jarch.athena.util.MulSemaphore.DALPermits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public class GrayUp {

    private static final Logger logger = LoggerFactory.getLogger(GrayUp.class);
    public volatile ServerSession serverSession;
    private volatile boolean started = false;
    public volatile boolean isReadySend = false;
    public volatile CmdQuery query;
    private final SqlSessionContext sqlCtx;
    private boolean isInShardingTrans = false;
    private String clientIp = "";
    private AtomicReference<DALPermits> semRef = new AtomicReference<>();

    public boolean isStarted() {
        return started;
    }

    public GrayUp(SqlSessionContext sqlCtx) {
        this.sqlCtx = sqlCtx;
    }

    public void setStarted(boolean started) {
        this.started = started;
    }

    public void returnResource() throws QuitException {
        CmdQuery query = sqlCtx.curCmdQuery;
        if (query == null) {
            logger.error("strange! CmdQuery query is null");
            return;
        }
        QUERY_TYPE type = query.queryType;
        if (type.isEndTrans() || !sqlCtx.isInTransStatus()) {
            returnServerSession();
        }
    }

    public void clearGrayUp() {
        if (started == false && AthenaConfig.getInstance().getGrayType() != GrayType.GRAY) {
            return;
        }
        try {
            releaseDbSemaphore();
            logger.info("Called clearGrayUp." + sqlCtx);
            started = false;
            closeServerSession();
            // query = null;
            if (isReadySend) {
                isReadySend = false;
                sqlCtx.setState(SESSION_STATUS.QUERY_ANALYZE);
                sqlCtx.clientWriteAndFlush(Unpooled.EMPTY_BUFFER);
            }
        } catch (Exception e) {
            logger.error("Unexpected Exception.", e);
        }
    }

    private synchronized void returnServerSession() {
        if (serverSession != null) {
            try {
                serverSession.returnServerSession();
                serverSession = null;
            } catch (QuitException e) {
                logger.error(Objects.toString(e));
            }
        }
    }

    private synchronized void closeServerSession() {
        if (serverSession != null) {
            serverSession.closeServerSession();
            serverSession = null;
        }
    }

    public boolean isInShardingTrans() {
        return isInShardingTrans;
    }

    public void setInShardingTrans(boolean isInShardingTrans) {
        this.isInShardingTrans = isInShardingTrans;
    }

    public String getSessionInfo() {
        ServerSession serverSession = this.serverSession;
        if (serverSession == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        if (sqlCtx.authenticator != null) {
            sb.append("user=").append(sqlCtx.authenticator.userName).append(" ");
        }
        sb.append("transId=").append(sqlCtx.transactionId).append(" ");
        if (sqlCtx.isInTransStatus()) {
            sb.append(sqlCtx.sqlSessionContextUtil.getTransTime()).append(" ");
        }
        sb.append(serverSession);
        return sb.toString();
    }

    public boolean isGrayReadSession() {
        return false;
    }

    public void releaseDbSemaphore() {
        DALPermits sem = semRef.getAndSet(null);
        if (sem != null) {
            sem.release();
            sqlCtx.scheduler = EmptyScheduler.emptySched;
        }
    }

    public void setDbSemaphore(DALPermits activeDBSessionsSemaphore) {
        DALPermits oldSem = semRef.getAndSet(activeDBSessionsSemaphore);
        if (oldSem != null) {
            oldSem.release();
        }
    }

    public void setClientAddr(String remoteAddr) {
        String[] items = remoteAddr.split(":");
        String ip = "";
        if (items.length > 0 && items[0].trim().length() > 1) {
            ip = items[0].trim().substring(1);
        }
        this.clientIp = ip;
    }
}
