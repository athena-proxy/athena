package me.ele.jarch.athena.server.async;

import me.ele.jarch.athena.allinone.DBConnectionInfo;
import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.pg.proto.*;
import me.ele.jarch.athena.server.pool.ServerSession.SERVER_SESSION_STATUS;
import me.ele.jarch.athena.sql.PGResultSet;
import me.ele.jarch.athena.sql.ResultSet;
import me.ele.jarch.athena.sql.ResultType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static me.ele.jarch.athena.server.pool.ServerSession.SERVER_SESSION_STATUS.AUTH_RESULT_SETAUTOCOMMIT;

public class PGAsyncHeartBeat extends AsyncHeartBeat {
    private static final Logger LOGGER = LoggerFactory.getLogger(PGAsyncHeartBeat.class);

    protected RowDescription rowDesc = null;
    protected final List<DataRow> dataRows = new LinkedList<DataRow>();

    protected PGAsyncHeartBeat(DBConnectionInfo dbConnInfo, AsyncResultSetHandler resultHandler) {
        this(dbConnInfo, resultHandler, 1000);
    }

    protected PGAsyncHeartBeat(DBConnectionInfo dbConnInfo, AsyncResultSetHandler resultHandler,
        long timeout) {
        super(dbConnInfo, resultHandler, timeout);
        // PG的连接是基于进程的,所以扩大其心跳连接生命周期
        super.time_to_live = TimeUnit.MINUTES.toMillis(30);
    }

    @Override protected AsyncSqlServerPacketDecoder newAsyncSqlServerPacketDecoder() {
        return new PGAsyncSqlServerPacketDecoder();
    }

    @Override protected String getQuery() {
        return "SELECT pg_is_in_recovery()";
    }

    @Override protected void doHandshakeAndAuth() throws QuitException {
        byte[] packet = serverPackets.poll();
        if (packet == null) {
            return;
        }
        Authentication auth = Authentication.loadFromPacket(packet);
        if (auth instanceof AuthenticationMD5Password) {
            PasswordMessage pm = new PasswordMessage(dbConnInfo.getUser(), dbConnInfo.getPword(),
                ((AuthenticationMD5Password) auth).getSalt());
            setStatus(AUTH_RESULT_SETAUTOCOMMIT);
            dbServerWriteAndFlush(pm.toPacket(), (future) -> {
                if (future.isSuccess()) {
                    return;
                }
                LOGGER.error(
                    "Cannot send query buf to server in handshakeAndAuth. " + this.dbConnInfo
                        .getQualifiedDbId());
                doQuit("Cannot send query buf to server in handshakeAndAuth.");
            });
        } else if (auth instanceof AuthenticationOk) {
            // 免密码登录,跳过认证阶段
            setStatus(AUTH_RESULT_SETAUTOCOMMIT);
            execute();
        } else {
            LOGGER.error("unsupported auth method:" + auth);
        }

    }

    @Override protected void doAuthResult() throws QuitException {
        // receive many packets
        byte[] packet = null;
        while (Objects.nonNull(packet = serverPackets.poll())) {
            switch (packet[0]) {
                case PGFlags.ERROR_RESPONSE:
                    LOGGER.error("received error when doAuth: {}",
                        ErrorResponse.loadFromPacket(packet));
                    doQuit("received error when doAuth");
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
                    if (LOGGER.isDebugEnabled())
                        LOGGER.debug(Objects.toString(bkd));
                    break;
                case PGFlags.READY_FOR_QUERY:
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(Objects.toString(ReadyForQuery.loadFromPacket(packet)));
                    }
                    setStatus(SERVER_SESSION_STATUS.QUERY);
                    this.execute();
                    break;
            }
        }
    }

    @Override protected void sendQueryToServer(String query) {
        dbServerWriteAndFlush(new Query(query).toPacket(), (future) -> {
            if (future.isSuccess()) {
                return;
            }
            LOGGER.error(
                "Cannot send QueryMessage to server in doQuery. " + dbConnInfo.getQualifiedDbId());
            doQuit("Cannot send QueryMessage to server in doQuery");
        });
    }

    @Override protected void doResult() throws QuitException {
        // receive many packets
        byte[] packet = null;
        while (Objects.nonNull(packet = serverPackets.poll())) {
            switch (packet[0]) {
                case PGFlags.ERROR_RESPONSE:
                    LOGGER.error("received error when doResult: {}",
                        ErrorResponse.loadFromPacket(packet));
                    doQuit("received error when doResult");
                    break;
                case PGFlags.ROW_DESCRIPTION:
                    rowDesc = RowDescription.loadFromPacket(packet);
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(Objects.toString(rowDesc));
                    }
                    break;
                case PGFlags.DATA_ROW:
                    DataRow dataRow = DataRow.loadFromPacket(packet);
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(Objects.toString(dataRow));
                    }
                    if (dataRow != null) {
                        dataRows.add(dataRow);
                    }
                    break;
                case PGFlags.COMMAND_COMPLETE:
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(Objects.toString(CommandComplete.loadFromPacket(packet)));
                    }
                    break;
                case PGFlags.READY_FOR_QUERY:
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(Objects.toString(ReadyForQuery.loadFromPacket(packet)));
                    }
                    handleResult();
                    setStatus(SERVER_SESSION_STATUS.QUERY);
                    break;
            }
        }
    }

    protected void handleResult() {
        if (task != null) {
            task.cancel(false);
            task = null;
        }
        this.isAvailable.set(true);
        handleResultSet(PGResultSet.newPGResultSet(rowDesc, dataRows, ResultType.RESULT_SET));
        rowDesc = null;
        dataRows.clear();
    }

    /**
     * @throws IndexOutOfBoundsException will throw if col/row is 0
     */
    @Override public boolean isReadOnly(ResultSet rs) {
        rs.next();
        return !"f".equals(rs.getString(1));
    }

    @Override protected byte[] newQuitPackets() {
        return Terminate.INSTANCE.toPacket();
    }
}
