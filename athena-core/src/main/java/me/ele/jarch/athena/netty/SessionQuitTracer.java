package me.ele.jarch.athena.netty;

import com.github.mpjct.jmpjct.util.ErrorCode;
import me.ele.jarch.athena.SQLLogFilter;
import me.ele.jarch.athena.constant.TraceNames;
import me.ele.jarch.athena.util.QueryStatistics;
import me.ele.jarch.athena.util.ResponseStatus;
import me.ele.jarch.athena.util.TransStatistics;
import me.ele.jarch.athena.util.etrace.MetricFactory;
import me.ele.jarch.athena.util.etrace.MetricMetaExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class SessionQuitTracer {
    private static final Logger logger = LoggerFactory.getLogger(SessionQuitTracer.class);


    public enum QuitTrace {
        ClientNormalQuit, ClientBroken, ServerBroken, Send2ClientBroken, Send2ServerBroken, DasMaxResultBufBroken, LoginFailure, NotQuit, ClientUnsync, ServerUnsync, AutoKillSlowTrans, AutoKillSlowSQL, ClientAuthenticationFailed, DALAbort, ManualKill, GlobalIdErr, DBFuse, ReadOnlyErr, DalGroupOffline, IllegalSavePoint;

        private static Map<QuitTrace, String> metricMap = new HashMap<>();

        static {
            metricMap.put(ClientNormalQuit, "conn.client.quit");
            metricMap.put(ClientBroken, "conn.client.broken");
            metricMap.put(ServerBroken, "conn.server.broken");
            metricMap.put(Send2ClientBroken, "conn.das.send2client.broken");
            metricMap.put(Send2ServerBroken, "conn.das.send2serve.broken");
            metricMap.put(DasMaxResultBufBroken, "conn.das.max_result_buf.broken");
            metricMap.put(LoginFailure, "conn.login.failure");
            metricMap.put(NotQuit, "conn.das.not_quit");
            metricMap.put(ClientUnsync, "conn.client.unsync.query");
            metricMap.put(AutoKillSlowSQL, "conn.autokill.slowsql.killed");
            metricMap.put(AutoKillSlowTrans, "conn.autokill.slowtrans.killed");
            metricMap.put(ClientAuthenticationFailed, "conn.das.authentication.failed");
            metricMap.put(DALAbort, "conn.das.abort");
            metricMap.put(ServerUnsync, "conn.server.unsync.query");
            metricMap.put(ManualKill, "conn.manualkill");
            metricMap.put(GlobalIdErr, "conn.globalid.err");
            metricMap.put(DBFuse, "conn.sql.sick");
            metricMap.put(ReadOnlyErr, "conn.readonly.err");
            metricMap.put(DalGroupOffline, "conn.dalgroup.offline");

        }

        @Override public String toString() {
            return metricMap.get(this);

        }
    }


    private QuitTrace recordedQuitTrace = QuitTrace.NotQuit;
    private final SqlSessionContext sqlCtx;

    public SessionQuitTracer(SqlSessionContext sqlCtx) {
        this.sqlCtx = sqlCtx;
    }

    /**
     * 通过QuitTrace设定ResponseStatus
     *
     * @param quitTrace
     */
    public synchronized void reportQuit(QuitTrace quitTrace) {
        ResponseStatus status = getResponseStatusAndLog(quitTrace);
        reportQuit(quitTrace, status);
    }

    /**
     * 设置QuitTrace并手动指定ResponseStatus
     * 每个SqlSessionContext应在reportQuit(QuitTrace quitTrace)
     * 和本方法之间任选其一调用
     *
     * @param quitTrace
     * @param status
     */
    public synchronized void reportQuit(QuitTrace quitTrace, ResponseStatus status) {
        if (this.recordedQuitTrace != QuitTrace.NotQuit) {
            return;
        }
        sqlCtx.sqlSessionContextUtil.trySetResponseStatus(status);

        this.recordedQuitTrace = quitTrace;
        MetricFactory.newCounter(quitTrace.toString())
            .addTag(TraceNames.DALGROUP, MetricMetaExtractor.extractDALGroupName(sqlCtx))
            .addTag(TraceNames.DBID, MetricMetaExtractor.extractDBid(sqlCtx)).once();
        if (this.sqlCtx.isInTransStatus()) {
            sqlCtx.transLog(sqlCtx.transactionId, 1);
        }
        if (sqlCtx.getCurQuery() != null && !sqlCtx.getCurQuery().isEmpty()) {
            long proxyDur = -1;
            long serverDur = -1;
            QueryStatistics qs = sqlCtx.sqlSessionContextUtil.getCurrentQueryStatistics();
            TransStatistics ts = sqlCtx.sqlSessionContextUtil.getTransStatistics();
            if (qs.getsSendTime() == 0) {
                SQLLogFilter.error("Failure ! sql was not sent to mysql yet," + sqlCtx.toString()
                    + ", quitTrace type=" + quitTrace);
            } else {
                serverDur = System.currentTimeMillis() - qs.getsSendTime();
                long transDur = ts.getTransationDur();
                SQLLogFilter.error(
                    "Failure ! sql last time:" + serverDur + "ms, transDur:" + transDur + "ms, "
                        + sqlCtx.toString() + ", quitTrace type=" + quitTrace);
            }
            proxyDur = System.currentTimeMillis() - qs.getcRecvTime();
            sqlCtx.sqlSessionContextUtil.doSQLLog(proxyDur, serverDur);
            sqlCtx.sqlSessionContextUtil.completeTransaction();
        } else if (this.recordedQuitTrace == QuitTrace.ClientBroken) {
            logger.warn(
                "ClientBroken ! sql context is quit, " + sqlCtx.toString() + ", quitTrace type="
                    + quitTrace);
        } else if (this.recordedQuitTrace != QuitTrace.ClientNormalQuit) {
            SQLLogFilter.error(
                "Failure ! sql context is quit, " + sqlCtx.toString() + ", quitTrace type="
                    + quitTrace);
        }
        sqlCtx.sqlSessionContextUtil.endEtrace();
    }

    private ResponseStatus getResponseStatusAndLog(QuitTrace quitTrace) {
        ResponseStatus status = new ResponseStatus(ResponseStatus.ResponseType.DALERR);
        switch (quitTrace) {
            case ClientNormalQuit:
                if (!sqlCtx.isInTransStatus() && sqlCtx.getCurQuery().isEmpty()) {
                    status = new ResponseStatus(ResponseStatus.ResponseType.OK,
                        ErrorCode.OK_CLIENT_NORMAL_QUIT, "ClientNormalQuit");
                } else {
                    status = new ResponseStatus(ResponseStatus.ResponseType.ABORT,
                        ErrorCode.ABORT_CLIENT_BROKEN, "ClientNormalQuitNonIdle");
                }
                logger.warn("Received QUIT instruction from client: " + sqlCtx.getClientInfo());
                break;
            case ClientBroken:
                if (!sqlCtx.isInTransStatus() && sqlCtx.getCurQuery().isEmpty()) {
                    status = new ResponseStatus(ResponseStatus.ResponseType.OK,
                        ErrorCode.OK_CLIENT_NORMAL_QUIT, "ClientBrokenIdle");
                } else {
                    status = new ResponseStatus(ResponseStatus.ResponseType.ABORT,
                        ErrorCode.ABORT_CLIENT_BROKEN, "ClientBroken");
                }
                logger.warn("Client inactive : " + sqlCtx.getClientInfo() + ",PacketTime: "
                    + sqlCtx.sqlSessionContextUtil.getPacketTime());
                break;
            case ServerBroken:
                status = new ResponseStatus(ResponseStatus.ResponseType.ABORT,
                    ErrorCode.ABORT_SERVER_BROKEN, "ServerBroken");
                logger.warn("Server inactive " + sqlCtx.sqlSessionContextUtil.getPacketTime());
                break;
            case Send2ClientBroken:
                status.setCode(ErrorCode.ERR_SEND_CLIENT_BROKEN).setMessage("Send2ClientBroken");
                logger.warn("Failed to write to client channel : " + sqlCtx.getClientInfo() + " "
                    + sqlCtx.sqlSessionContextUtil.getPacketTime());
                break;
            case Send2ServerBroken:
                status.setCode(ErrorCode.ERR_SEND_SERVER_BROKEN).setMessage("Send2ServerBroken");
                logger.warn("Failed to write to server channel");
                break;
            case LoginFailure:
                status.setCode(ErrorCode.ERR_LOGIN_FAILURE).setMessage("LoginFailure");
                logger.error("Failed to log onto the dbserver"); // to do: add more server details
                break;
            case ClientUnsync:
                status.setCode(ErrorCode.ERR_CLIENT_UNSYNC).setMessage("ClientUnsync");
                logger
                    .error("Received query from client at QUERY_RESULT state " + sqlCtx.toString());
                break;
            case DasMaxResultBufBroken:
                String msg = "SQL Result set is larger than " + sqlCtx.getHolder().getZKCache()
                    .getMaxResultBufSize() + ". Abort this connection to protect the resources.";
                msg += " " + sqlCtx.sqlSessionContextUtil.getPacketTime();
                msg += " The original SQL \"" + sqlCtx.getCurQuery() + "\"";
                status.setCode(ErrorCode.ERR_OVER_MAX_RESULT_BUF_SIZE)
                    .setMessage("DasMaxResultBufBroken");
                SQLLogFilter.error(msg);
                break;
            case AutoKillSlowSQL:
                status = new ResponseStatus(ResponseStatus.ResponseType.ABORT,
                    ErrorCode.ABORT_AUTOKILL_SLOWSQL, "AutoKillSlowSQL");
                long serverDur =
                    System.currentTimeMillis() - sqlCtx.sqlSessionContextUtil.getQueryStatistics()
                        .getsSendTime();
                logger.error("Auto kill slow sql, serverDur=" + serverDur + " "
                    + sqlCtx.sqlSessionContextUtil.getPacketTime() + ", query: " + sqlCtx
                    .getCurQuery());
                break;
            case AutoKillSlowTrans:
                status = new ResponseStatus(ResponseStatus.ResponseType.ABORT,
                    ErrorCode.ABORT_AUTOKILL_SLOWTRANS, "AutoKillSlowTrans");
                TransStatistics ts = sqlCtx.sqlSessionContextUtil.getTransStatistics();
                long transDur = System.currentTimeMillis() - ts.gettStartTime();
                long transIdleDur =
                    System.currentTimeMillis() - ts.getlastQueryStatistics().getcSendTime();
                logger.error("Auto kill slow trans, transIdleDur=" + transIdleDur + " transId=" + ts
                    .getTransId() + " lastQueryData:" + ts.getlastQueryStatistics() + " transDur="
                    + transDur + " " + sqlCtx);
                break;
            case DBFuse:
                status = new ResponseStatus(ResponseStatus.ResponseType.DALERR, ErrorCode.DALSICK,
                    "DBFuse");
                logger.error("DB sick query: " + sqlCtx.getCurQuery());
                break;
            case ClientAuthenticationFailed:
                status = new ResponseStatus(ResponseStatus.ResponseType.ABORT,
                    ErrorCode.ABORT_AUTHENTICATION_FAILED, "ClientAuthenticationFailed");
                logger.error("Client Authentication Failed");
                break;
            case ServerUnsync:
                status = new ResponseStatus(ResponseStatus.ResponseType.ABORT,
                    ErrorCode.ABORT_SERVER_UNSYNC, "ServerUnsync");
                logger.error("Received query from server at QUERY_ANALYZE or QUERY_HANDLE state");
                break;
            case ManualKill:
                status = new ResponseStatus(ResponseStatus.ResponseType.ABORT,
                    ErrorCode.ABORT_MANUAL_KILL, "ManualKill");
                logger.error("Session is killed by admin, " + sqlCtx);
                break;
            case GlobalIdErr:
                status =
                    new ResponseStatus(ResponseStatus.ResponseType.ABORT, ErrorCode.ABORT_GLOBAL_ID,
                        "GlobalIdErr");
                logger.error("Session is killed by admin, " + sqlCtx);
                break;
            case ReadOnlyErr:
                status = new ResponseStatus(ResponseStatus.ResponseType.ABORT,
                    ErrorCode.ABORT_READ_ONLY_MODE, "ReadOnlyErr");
                logger.error("Session is killed by admin, " + sqlCtx);
                break;
            default:
                break;
        }
        return status;
    }

    public QuitTrace getRecordedQuitTrace() {
        return recordedQuitTrace;
    }
}
