package me.ele.jarch.athena.netty.state;

import com.github.mpjct.jmpjct.util.ErrorCode;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.exception.AuthQuitException;
import me.ele.jarch.athena.exception.AuthQuitState;
import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.netty.PGSqlSessionContext;
import me.ele.jarch.athena.pg.proto.*;
import me.ele.jarch.athena.pg.util.PGAuthenticator;
import me.ele.jarch.athena.pg.util.Severity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class PGClientFakeAuthState extends ClientFakeAuthState {
    private static final Logger LOGGER = LoggerFactory.getLogger(PGClientFakeAuthState.class);
    private final PGAuthenticator auth;

    public PGClientFakeAuthState(PGSqlSessionContext sqlSessionContext) {
        super(sqlSessionContext);
        auth = (PGAuthenticator) sqlSessionContext.authenticator;
    }

    @Override public boolean handle() throws QuitException {
        return doClientAuthResult();
    }

    @Override public SESSION_STATUS getStatus() {
        return SESSION_STATUS.CLIENT_FAKE_AUTH;
    }

    private List<ParameterStatus> buildParameterStatus() {
        // session_authorization这个字段理应和user保持一致,
        // 但DAL有自己的登陆user,之后可能有客户端会check该值,所以没有传入
        List<ParameterStatus> list = new ArrayList<>();
        String application_name = auth.getClientAttributes().getOrDefault("application_name", "");
        list.add(new ParameterStatus("application_name", application_name));
        list.add(new ParameterStatus("client_encoding", "UTF8"));
        list.add(new ParameterStatus("DateStyle", "ISO, MDY"));
        list.add(new ParameterStatus("integer_datetimes", "on"));
        list.add(new ParameterStatus("IntervalStyle", "postgres"));
        list.add(new ParameterStatus("server_encoding", "UTF8"));
        list.add(new ParameterStatus("server_version", Constants.PG_SERVER_VERSION));
        list.add(new ParameterStatus("standard_conforming_strings", "on"));
        list.add(new ParameterStatus("TimeZone", "PRC"));
        return list;
    }

    private boolean checkAuth(byte[] packet) {
        // 没有该DalGroup,返回失败
        if (Objects.isNull(sqlSessionContext.getHolder())) {
            String errMsg = String.format("database '%s' does not exist", auth.getSchema());
            auth.addAuthFailedReason(String
                .format("No available dalGroup Dispatcher for user %s@%s", auth.userName,
                    auth.getSchema()));
            auth.setState(AuthQuitState.ILLEGAL_DAL_GROUP);
            auth.setErrorResponse(buildDBNotExistErr(errMsg));
            return false;
        }
        if (!auth.checkPrivileges()) {
            String errMsg =
                String.format("privileges authentication failed for user '%s'", auth.userName);
            auth.addAuthFailedReason(errMsg);
            auth.setErrorResponse(buildAuthFailedErr(errMsg));
            return false;
        }
        // 校验用户密码是否OK
        if (auth.checkPassword(PasswordMessage.loadFromPacket(packet).getPassword())) {
            return true;
        }
        // 验证不成功，返回错误
        String errMsg =
            String.format("password authentication failed for user '%s'", auth.userName);
        auth.addAuthFailedReason(errMsg);
        auth.setErrorResponse(buildAuthFailedErr(errMsg));
        return false;
    }

    private boolean doClientAuthResult() throws QuitException {
        byte[] packet = sqlSessionContext.clientPackets.poll();
        auth.isAuthenticated = checkAuth(packet);
        if (auth.isAuthenticated) {
            sqlSessionContext.setState(SESSION_STATUS.QUERY_ANALYZE);
            // Auth OK,backend key data,ready for query
            ByteBuf buf = Unpooled.buffer();
            buf.writeBytes(AuthenticationOk.INSTANCE.toPacket());
            buildParameterStatus().forEach(ps -> buf.writeBytes(ps.toPacket()));
            buf.writeBytes(
                ((PGSqlSessionContext) sqlSessionContext).getBackendKeyData().toPacket());
            buf.writeBytes(ReadyForQuery.IDLE.toPacket());
            sqlSessionContext.clientWriteAndFlush(buf, future -> {
                // 为了更精确的追踪时间,将log以及etrace记录放在数据包写入网络栈后处理
                LOGGER.info(String.format(
                    "Succeeded to authenticate user '%s@%s'  from '%s' with client type '%s', bind2master=%b",
                    auth.userName, auth.getSchema(), sqlSessionContext.getClientInfo(),
                    auth.getClientAttributes(), sqlSessionContext.bind2master));
                sqlSessionContext.sqlSessionContextUtil.appendLoginPhrase(
                    future.isSuccess() ? Constants.AUTH_OK_SENT : Constants.AUTH_OK_BROKEN)
                    .appendLoginTimeLineEtrace().endEtrace();
            });
        } else {
            sqlSessionContext.sqlSessionContextUtil.appendLoginPhrase(Constants.AUTH_ERR)
                .appendLoginTimeLineEtrace();
            sqlSessionContext
                .clientWriteAndFlush(Unpooled.wrappedBuffer(auth.getErrorResponse().toPacket()),
                    future -> LOGGER.error(
                        "Failed to authenticate user '{}@{}'  from '{}' with client type '{}', bind2master={}",
                        auth.userName, auth.getSchema(), sqlSessionContext.getClientInfo(),
                        auth.getClientAttributes(), sqlSessionContext.bind2master));
            throw new AuthQuitException(auth.getAuthFailedReason(), auth.getState());
        }
        return false;
    }

    private ErrorResponse buildAuthFailedErr(String msg) {
        return ErrorResponse
            .buildErrorResponse(Severity.FATAL, ErrorCode.PG_INVALID_PASSWORD.getSqlState(), msg,
                getClass().getSimpleName(), "1", "auth_failed");
    }

    private ErrorResponse buildDBNotExistErr(String msg) {
        return ErrorResponse
            .buildErrorResponse(Severity.FATAL, ErrorCode.PG_INVALID_CATALOG_NAME.getSqlState(),
                msg, getClass().getSimpleName(), "1", "InitPostres");
    }
}
