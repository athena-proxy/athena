package me.ele.jarch.athena.netty.state;

import com.github.mpjct.jmpjct.util.ErrorCode;
import io.netty.buffer.Unpooled;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.constant.Metrics;
import me.ele.jarch.athena.constant.TraceNames;
import me.ele.jarch.athena.exception.AuthQuitException;
import me.ele.jarch.athena.exception.AuthQuitState;
import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.netty.AthenaFrontServer;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.scheduler.DBChannelDispatcher;
import me.ele.jarch.athena.sql.CmdAuthFake;
import me.ele.jarch.athena.sql.CmdAuthResultFake;
import me.ele.jarch.athena.util.ResponseStatus;
import me.ele.jarch.athena.util.etrace.MetricFactory;
import me.ele.jarch.athena.util.etrace.MetricMetaExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;

public class ClientFakeAuthState implements State {
    private static final Logger logger = LoggerFactory.getLogger(ClientFakeAuthState.class);
    protected SqlSessionContext sqlSessionContext;

    public ClientFakeAuthState(SqlSessionContext sqlSessionContext) {
        this.sqlSessionContext = sqlSessionContext;
    }

    @Override public boolean handle() throws QuitException {
        return doClientAuthResult();
    }

    @Override public SESSION_STATUS getStatus() {
        return SESSION_STATUS.CLIENT_FAKE_AUTH;
    }

    private boolean doClientAuthResult() throws QuitException {
        byte[] packet = sqlSessionContext.clientPackets.poll();
        CmdAuthFake auth = new CmdAuthFake(packet);
        auth.execute();

        sqlSessionContext.authenticator
            .setUserAndPassWord(auth.userName, auth.passwordAfterEncryption, auth.schema);
        CmdAuthResultFake authResult = new CmdAuthResultFake(sqlSessionContext.authenticator);
        authResult.execute();

        if (sqlSessionContext.authenticator.isDalHealthCheckUser) {
            if (!AthenaFrontServer.getInstance().isAcceptInComeConnection()) {
                authResult.buildDalHeartBeatError();
                sqlSessionContext.sqlSessionContextUtil.trySetResponseStatus(
                    new ResponseStatus(ResponseStatus.ResponseType.DALERR, ErrorCode.DAL_SHUTDOWN,
                        "dal service ports closed"));
            }
            sqlSessionContext.clientWriteAndFlush(Unpooled.wrappedBuffer(authResult.getSendBuf()));
            sqlSessionContext.setState(SESSION_STATUS.QUIT);
            sqlSessionContext.sqlSessionContextUtil.continueHealthCheckEtrace(auth.userName);
            sqlSessionContext.sqlSessionContextUtil.endEtrace();
            return false;
        }

        DBChannelDispatcher tmpDBChannelDispatcher = getDispatcher(auth.userName, auth.schema,
            sqlSessionContext.authenticator.isLegalSchema());
        if (tmpDBChannelDispatcher != null && !tmpDBChannelDispatcher.isOffline()) {
            sqlSessionContext.setDBChannelDispatcher(tmpDBChannelDispatcher);
            sqlSessionContext.shardingContext =
                sqlSessionContext.newShardingContext(sqlSessionContext.quitTracer);
            sqlSessionContext.sqlClientPacketDecoder.holder = tmpDBChannelDispatcher;
            tmpDBChannelDispatcher.cCnt.incrementAndGet();
            tmpDBChannelDispatcher.opencCnt.incrementAndGet();
            sqlSessionContext.setState(SESSION_STATUS.QUERY_ANALYZE);
        } else {
            authResult.buildSchemaError();
            String error4NoDispatcher = String
                .format("No available dalGroup Dispatcher for user %s@%s", auth.userName,
                    auth.schema);
            logger.error(error4NoDispatcher);
            sqlSessionContext.authenticator.addAuthFailedReason(error4NoDispatcher);
            sqlSessionContext.authenticator.setState(AuthQuitState.ILLEGAL_DAL_GROUP);
        }

        sqlSessionContext.sqlSessionContextUtil
            .continueLoginEtrace(auth.userName, tmpDBChannelDispatcher);
        sqlSessionContext
            .clientWriteAndFlush(Unpooled.wrappedBuffer(authResult.getSendBuf()), future -> {
                if (!sqlSessionContext.authenticator.isAuthenticated) {
                    logger.error(
                        "Failed to authenticate user '{}@{}'  from '{}' with client type '{}', bind2master={}",
                        auth.userName, auth.schema, sqlSessionContext.getClientInfo(),
                        auth.clientAttributes, sqlSessionContext.bind2master);
                    return;
                }
                logger.info(
                    "Succeeded to authenticate user '{}@{}'  from '{}' with client type '{}', bind2master={}",
                    auth.userName, auth.schema, sqlSessionContext.getClientInfo(),
                    auth.clientAttributes, sqlSessionContext.bind2master);
                sqlSessionContext.sqlSessionContextUtil.appendLoginPhrase(
                    future.isSuccess() ? Constants.AUTH_OK_SENT : Constants.AUTH_OK_BROKEN)
                    .appendLoginTimeLineEtrace().endEtrace();
            });

        if (auth.clientAttributes.contains("mysql")) {
            MetricFactory.newCounter(Metrics.LOGIN_MYSQL_CLIENT).addTag(TraceNames.DALGROUP,
                MetricMetaExtractor.extractDALGroupName(sqlSessionContext)).once();
        }
        if (!sqlSessionContext.authenticator.isAuthenticated) {
            sqlSessionContext.sqlSessionContextUtil.appendLoginPhrase(Constants.AUTH_ERR)
                .appendLoginTimeLineEtrace();
            throw new AuthQuitException(sqlSessionContext.authenticator.getAuthFailedReason(),
                sqlSessionContext.authenticator.getState());
        }
        return false;
    }

    static DBChannelDispatcher getDispatcher(String userName, String schema,
        final boolean isLegalSchema) {
        Optional<String> dalGroupOp =
            DBChannelDispatcher.getHolders().keySet().stream().filter(groupName -> {
                if (isLegalSchema) {
                    return groupName.equals(schema);
                }
                if (userName.contains("__sub__")) {
                    return userName.startsWith(groupName) && groupName.contains("__sub__");
                }
                return userName.startsWith(groupName);
            }).findFirst();
        if (dalGroupOp.isPresent()) {
            return getInitializedHolder(dalGroupOp.get());
        }

        if (!(Constants.ATHENA_ADMIN.equals(userName)) && !(Constants.ATHENA_ADMIN_READONLY
            .equals(userName))) {
            return null;
        }

        if (schema != null && !schema.isEmpty()) {
            dalGroupOp = DBChannelDispatcher.getHolders().keySet().stream()
                .filter(groupName -> groupName.equals(schema)).findFirst();
            if (dalGroupOp.isPresent()) {
                return getInitializedHolder(dalGroupOp.get());
            } else {
                return null;
            }

        }

        Optional<Map.Entry<String, DBChannelDispatcher>> tmpDBChannelDispatcher =
            DBChannelDispatcher.getHolders().entrySet().stream()
                .filter(entry -> entry.getValue().isDalGroupInitialized()).findAny();
        if (tmpDBChannelDispatcher.isPresent()) {
            return tmpDBChannelDispatcher.get().getValue();
        }

        return null;
    }

    private static DBChannelDispatcher getInitializedHolder(String channel) {
        DBChannelDispatcher holder = DBChannelDispatcher.getHolders().get(channel);
        if (holder != null && !holder.isOffline() && holder.isDalGroupInitialized()) {
            return holder;
        }
        return null;
    }

}
