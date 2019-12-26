package me.ele.jarch.athena.util.health.check;

import me.ele.jarch.athena.allinone.DBConnectionInfo;
import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.pg.proto.*;
import me.ele.jarch.athena.server.async.AsyncResultSetHandler;
import me.ele.jarch.athena.server.async.PGAsyncHeartBeat;
import me.ele.jarch.athena.server.pool.ServerSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class PGHealthCheckClient extends PGAsyncHeartBeat {
    private static final Logger LOGGER = LoggerFactory.getLogger(PGHealthCheckClient.class);


    private volatile String sql = "";

    protected PGHealthCheckClient(DBConnectionInfo dbConnInfo, AsyncResultSetHandler resultHandler,
        long timeout) {
        super(dbConnInfo, resultHandler, timeout);
    }

    @Override protected String getQuery() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getSql() {
        return sql;
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
                    setStatus(ServerSession.SERVER_SESSION_STATUS.QUERY);
                    handleResult();
                    break;
            }
        }
    }
}
