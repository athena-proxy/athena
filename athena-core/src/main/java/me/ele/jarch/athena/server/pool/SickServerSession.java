package me.ele.jarch.athena.server.pool;

import com.github.mpjct.jmpjct.mysql.proto.ERR;
import com.github.mpjct.jmpjct.mysql.proto.OK;
import com.github.mpjct.jmpjct.util.ErrorCode;
import io.netty.channel.embedded.EmbeddedChannel;
import me.ele.jarch.athena.allinone.DBConnectionInfo;
import me.ele.jarch.athena.netty.AthenaServer;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.util.ResponseStatus;
import me.ele.jarch.athena.worker.SchedulerWorker;
import me.ele.jarch.athena.worker.manager.ServerDecoderManager;

import java.util.ArrayDeque;
import java.util.LinkedList;
import java.util.Queue;

/**
 * Created by jinghao.wang on 16/3/1.
 */
public class SickServerSession extends ServerSession {
    protected static final String sickMessageTemplate = "sql sick '%s'";

    protected SickServerSession(DBConnectionInfo info, ServerSessionPool pool) {
        super(info, pool);
        skip2OKStatus();
        serverChannel = new EmbeddedChannel(EMPTY_HANDLER);
    }

    private void skip2OKStatus() {
        Queue<byte[]> queue = new LinkedList<>();
        queue.add(OK.getOkPacketBytes());
        setServerPackets(queue);
        setStatus(SERVER_SESSION_STATUS.RECEIVE_LAST_OK);
    }

    @Override
    public void dbServerWriteAndFlush(byte[] bytesBuf, ServerSessionErrorHandler handler) {
        SqlSessionContext sqlSessionContext = sqlServerPackedDecoder.getSqlCtx();
        String sickMessage = String.format(sickMessageTemplate, activeQulifiedDbId);
        sqlSessionContext.sqlSessionContextUtil.trySetResponseStatus(
            new ResponseStatus(ResponseStatus.ResponseType.DALERR, ErrorCode.DALSICK, sickMessage));
        int delayTime = sqlSessionContext.getHolder().getZKCache().getAutoKillerSlowSQL() / 2;
        AthenaServer.quickJobScheduler
            .addOneTimeJob("sql.sick" + activeQulifiedDbId, delayTime, () -> {
                SchedulerWorker.getInstance().enqueue(new ServerDecoderManager(sqlSessionContext,
                    buildErr(sickMessage, sqlSessionContext.isInTransStatus()), this));
            });
    }

    protected Queue<byte[]> buildErr(String sickMessage, boolean sqlCtxIsInTransStatus) {
        Queue<byte[]> queue = new ArrayDeque<>();
        queue.add(ERR.buildErr(1, ErrorCode.DALSICK.getErrorNo(), sickMessage).toPacket());
        return queue;
    }

    @Override public boolean isOverdue() {
        return false;
    }

    public static SickServerSession newSickServerSession(ServerSessionPool pool) {
        DBConnectionInfo info = pool.getDbConnInfo();
        switch (info.getDBVendor()) {
            case MYSQL:
                return new SickServerSession(info, pool);
            case PG:
                return new PGSickServerSession(info, pool);
            default:
                return new SickServerSession(info, pool);
        }
    }
}
