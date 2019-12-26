package me.ele.jarch.athena.server.pool;

import com.github.mpjct.jmpjct.util.ErrorCode;
import me.ele.jarch.athena.allinone.DBConnectionInfo;
import me.ele.jarch.athena.pg.proto.ErrorResponse;
import me.ele.jarch.athena.pg.proto.ReadyForQuery;
import me.ele.jarch.athena.pg.util.Severity;

import java.util.ArrayDeque;
import java.util.Queue;

public class PGSickServerSession extends SickServerSession {

    protected PGSickServerSession(DBConnectionInfo info, ServerSessionPool pool) {
        super(info, pool);
    }

    @Override protected Queue<byte[]> buildErr(String sickMessage, boolean sqlCtxIsInTransStatus) {
        Queue<byte[]> queue = new ArrayDeque<>();
        queue.add(ErrorResponse
            .buildErrorResponse(Severity.FATAL, ErrorCode.DALSICK.getSqlState(), sickMessage,
                getClass().getSimpleName(), "1", "sql_sick").toPacket());
        queue.add(sqlCtxIsInTransStatus ?
            ReadyForQuery.IN_FAILED_TX.toPacket() :
            ReadyForQuery.IDLE.toPacket());
        return queue;
    }

}
