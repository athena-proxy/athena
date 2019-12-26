package me.ele.jarch.athena.netty.state;

import com.github.mpjct.jmpjct.util.ErrorCode;
import io.netty.buffer.Unpooled;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.netty.AthenaFrontServer;
import me.ele.jarch.athena.netty.PGSqlSessionContext;
import me.ele.jarch.athena.netty.SessionQuitTracer;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.pg.proto.*;
import me.ele.jarch.athena.pg.util.PGAuthenticator;
import me.ele.jarch.athena.pg.util.Severity;
import me.ele.jarch.athena.scheduler.DBChannelDispatcher;
import me.ele.jarch.athena.scheduler.HangSessionMonitor;
import me.ele.jarch.athena.util.ResponseStatus;
import me.ele.jarch.athena.worker.SchedulerWorker;
import me.ele.jarch.athena.worker.manager.ManualKillerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class PGClientFakeHandshakeState extends ClientFakeHandshakeState {
    private static final Logger LOGGER = LoggerFactory.getLogger(PGClientFakeHandshakeState.class);
    private static final byte[] SSL_NOT_SUPPORTED_BUF = {'N'};
    private final PGAuthenticator auth;

    public PGClientFakeHandshakeState(PGSqlSessionContext sqlSessionContext) {
        super(sqlSessionContext);
        this.auth = (PGAuthenticator) sqlSessionContext.authenticator;
    }

    @Override public boolean handle() throws QuitException {
        return doClientHandshake();
    }

    @Override public SESSION_STATUS getStatus() {
        return SESSION_STATUS.CLIENT_FAKE_HANDSHAKE;
    }

    private boolean doClientHandshake() throws QuitException {
        byte[] packet = sqlSessionContext.clientPackets.poll();
        if (packet[0] == PGFlags.C_SSL_REQUEST) {
            // 处理SSL请求并拒绝,让其走非ssl通道
            handleSSLRequest();
            return false;
        } else if (packet[0] == PGFlags.C_STARTUP_MESSAGE) {
            // 处理StartupMessage并且回复Auth请求
            handleStartupMessage(packet);
            return false;
        } else if (packet[0] == PGFlags.C_CANCEL_REQUEST) {
            handleCancelRequest(packet);
        }
        return false;
    }

    private void handleSSLRequest() {
        LOGGER.debug("doClientHandshake::SSL");
        sqlSessionContext.clientWriteAndFlush(Unpooled.wrappedBuffer(SSL_NOT_SUPPORTED_BUF),
            future -> sqlSessionContext.sqlSessionContextUtil
                .appendLoginPhrase(Constants.PG_NO_SSL_SENT));
    }

    private void handleStartupMessage(byte[] packet) {
        auth.setAddress(sqlSessionContext.getClientAddr());
        sqlSessionContext.sqlSessionContextUtil.startLoginEtrace();
        StartupMessage startupMessage = StartupMessage.loadFromVirtualPacket(packet);
        auth.setClientAttributes(startupMessage.getArgs());
        auth.setUserName(startupMessage.getUser());
        auth.setSchema(startupMessage.getDatabase());

        if (sqlSessionContext.authenticator.isDalHealthCheckUser) {
            /*
             * For the health check between athena and goproxy,
             * Goproxy sends a startup message, including username "dal_heartbeat", to athena;
             * athena replies with an AuthenticationOK message.
             * Thus, a health check is done.
             * */
            handleGoProxyHealthCheck();
            return;
        }

        DBChannelDispatcher dispatcher = ClientFakeAuthState
            .getDispatcher(startupMessage.getUser(), startupMessage.getDatabase(), true);
        if (Objects.nonNull(dispatcher)) {
            sqlSessionContext.setDBChannelDispatcher(dispatcher);
            sqlSessionContext.sqlClientPacketDecoder.holder = dispatcher;
            sqlSessionContext.shardingContext =
                sqlSessionContext.newShardingContext(sqlSessionContext.quitTracer);
            dispatcher.cCnt.incrementAndGet();
            dispatcher.opencCnt.incrementAndGet();
        }
        sqlSessionContext.sqlSessionContextUtil
            .continueLoginEtrace(startupMessage.getUser(), dispatcher);
        sqlSessionContext.setState(SESSION_STATUS.CLIENT_FAKE_AUTH);
        // currently, only support MD5
        AuthenticationMD5Password authen = new AuthenticationMD5Password();
        auth.setAuthSeed(authen.getSalt());
        sqlSessionContext.clientWriteAndFlush(Unpooled.wrappedBuffer(authen.toPacket()),
            future -> sqlSessionContext.sqlSessionContextUtil
                .appendLoginPhrase(Constants.PG_AUTH_METHOD_SENT));
    }

    private void handleGoProxyHealthCheck() {
        byte[] responsePacket = AuthenticationOk.INSTANCE.toPacket();
        if (!AthenaFrontServer.getInstance().isAcceptInComeConnection()) {
            responsePacket = ErrorResponse
                .buildErrorResponse(Severity.FATAL, ErrorCode.DAL_SHUTDOWN.getSqlState(),
                    "pg heart beat error", getClass().getSimpleName(), "1",
                    "Server shutdown in progress").toPacket();

            sqlSessionContext.sqlSessionContextUtil.trySetResponseStatus(
                new ResponseStatus(ResponseStatus.ResponseType.DALERR, ErrorCode.DAL_SHUTDOWN,
                    "dal service ports closed"));
        }
        sqlSessionContext.clientWriteAndFlush(Unpooled.wrappedBuffer(responsePacket));
        sqlSessionContext.setState(SESSION_STATUS.QUIT);
        sqlSessionContext.sqlSessionContextUtil.continueHealthCheckEtrace(auth.userName);
        sqlSessionContext.sqlSessionContextUtil.endEtrace();
    }

    private void handleCancelRequest(byte[] packet) {
        CancelRequest cr = CancelRequest.loadFromVirtualPacket(packet);
        LOGGER.warn("CancelRequest is received: {}", cr);
        for (SqlSessionContext ctx : HangSessionMonitor.getAllCtx()) {
            if (!(ctx instanceof PGSqlSessionContext)) {
                continue;
            }
            BackendKeyData bkd = ((PGSqlSessionContext) ctx).getBackendKeyData();
            if (cr.getKey() == bkd.getKey() && cr.getPid() == bkd.getPid()) {
                String errMsg = String.format(
                    "canceling statement due to user request, Session is killed by clientConnId=%d use cancel request",
                    sqlSessionContext.connectionId);
                // 中断Key,Pid为CancelRequest的那个连接
                SchedulerWorker.getInstance().enqueue(
                    new ManualKillerManager(ctx, SessionQuitTracer.QuitTrace.ManualKill,
                        new ResponseStatus(ResponseStatus.ResponseType.ABORT,
                            ErrorCode.ABORT_MANUAL_KILL, errMsg), ErrorCode.ABORT_MANUAL_KILL,
                        errMsg));
                break;
            }
        }
        // 并且关闭发送CancelRequest的那个连接
        sqlSessionContext.doQuit();
    }
}
