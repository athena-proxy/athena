package me.ele.jarch.athena.server.pool;

import me.ele.jarch.athena.allinone.DBConnectionInfo;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.netty.PGSqlServerPacketDecoder;
import me.ele.jarch.athena.netty.SqlServerPacketDecoder;
import me.ele.jarch.athena.pg.proto.*;
import me.ele.jarch.athena.util.KVChainParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static me.ele.jarch.athena.server.pool.ServerSession.SERVER_SESSION_STATUS.AUTH_RESULT_SETAUTOCOMMIT;

public class PGServerSession extends ServerSession {
    private static final Logger LOGGER = LoggerFactory.getLogger(PGServerSession.class);

    // 默认值为 60 分钟 + (0-120)分钟
    static final long DEFAULT_OVERDUE_IN_MINUTES = 60;


    protected PGServerSession(DBConnectionInfo info, ServerSessionPool pool) {
        super(info, pool);
    }

    @Override protected long getOverdueInMinutes() {
        long overdueInMinutes = DEFAULT_OVERDUE_IN_MINUTES;
        try {
            overdueInMinutes = Long.parseLong(this.dbConnInfo.getAdditionalDalCfg()
                .getOrDefault(Constants.PG_DB_OVERDUE_IN_MINUTES,
                    String.valueOf(DEFAULT_OVERDUE_IN_MINUTES)));
        } catch (NumberFormatException e) {
            LOGGER.error("", e);
        }
        return overdueInMinutes;
    }

    @Override protected SqlServerPacketDecoder newDecoder() {
        return new PGSqlServerPacketDecoder();
    }

    @Override protected void addAdditionalSessionCfg(DBConnectionInfo info) {
        additionSessionCfgQueue.add("client_encoding=UTF8");
        additionSessionCfgQueue.add("extra_float_digits=2");
        additionSessionCfgQueue.add("DateStyle=ISO, MDY");
        additionSessionCfgQueue.add("application_name=DAL");
        info.getAdditionalSessionCfg()
            .forEach((k, v) -> additionSessionCfgQueue.add(String.format("%s=%s", k, v)));
    }

    @Override public void returnServerSession() throws QuitException {
        pool.returnServerSession(this);
        releaseTrxSemaphore();
    }

    public byte[] newStartupMessage() {
        StartupMessage startupMessage =
            new StartupMessage(dbConnInfo.getUser(), dbConnInfo.getDatabase());
        KVChainParser.parse(additionSessionCfgQueue)
            .forEach((k, v) -> startupMessage.addArgs(k, v));
        return startupMessage.toPacket();
    }

    @Override protected boolean doHandshakeAndAuth() throws QuitException {
        byte[] packet = serverPackets.poll();
        Authentication auth = Authentication.loadFromPacket(packet);
        if (auth instanceof AuthenticationMD5Password) {
            PasswordMessage pm = new PasswordMessage(dbConnInfo.getUser(), dbConnInfo.getPword(),
                ((AuthenticationMD5Password) auth).getSalt());
            setStatus(SERVER_SESSION_STATUS.AUTH_RESULT_SETAUTOCOMMIT);
            dbServerWriteAndFlush(pm.toPacket(), (s) -> {
                LOGGER.error("Cannot send query buf to server in sendQuery. " + this.dbConnInfo
                    .getQualifiedDbId());
                doQuit();
            });
        } else if (auth instanceof AuthenticationOk) {
            // 免密码登录,跳过认证阶段
            setStatus(AUTH_RESULT_SETAUTOCOMMIT);
            execute();
        } else {
            LOGGER.error("unsupported auth method:" + auth);
        }
        return false;
    }

    @Override protected void doAuthResultAndSetAutocommit() throws QuitException {
        // receive many packets
        byte[] packet = null;
        while (Objects.nonNull(packet = serverPackets.poll())) {
            switch (packet[0]) {
                case PGFlags.ERROR_RESPONSE:
                    LOGGER.error("received error when create session: {}",
                        ErrorResponse.loadFromPacket(packet));
                    doQuit();
                    break;
                case PGFlags.AUTHENTICATION_OK:
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(Objects.toString(AuthenticationOk.loadFromPacket(packet)));
                    }
                    break;
                case PGFlags.PARAMETER_STATUS:
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(Objects.toString(ParameterStatus.loadFromPacket(packet)));
                    }
                    break;
                case PGFlags.BACKEND_KEY_DATA:
                    BackendKeyData bkd = BackendKeyData.loadFromPacket(packet);
                    this.connectionId = bkd.getPid();
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(Objects.toString(bkd));
                    }
                    break;
                case PGFlags.READY_FOR_QUERY:
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(Objects.toString(ReadyForQuery.loadFromPacket(packet)));
                    }
                    isReady = true;
                    handleReady();
                    break;
            }
        }
    }

    @Override protected byte[] newQuitPackets() {
        return Terminate.INSTANCE.toPacket();
    }
}
