package me.ele.jarch.athena.worker.manager;

import com.github.mpjct.jmpjct.util.ErrorCode;
import me.ele.jarch.athena.allinone.DBRole;
import me.ele.jarch.athena.netty.SessionQuitTracer;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.netty.state.SESSION_STATUS;
import me.ele.jarch.athena.server.pool.ServerSession;
import me.ele.jarch.athena.util.QueryStatistics;
import me.ele.jarch.athena.worker.EventSessionTask;
import me.ele.jarch.athena.worker.TaskPriority;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Queue;

public class ServerDecoderManager extends Manager {

    private static final Logger logger = LoggerFactory.getLogger(ServerDecoderManager.class);
    final Queue<byte[]> packets;
    final ServerSession serverSession;
    final long receiveTime;

    public ServerDecoderManager(SqlSessionContext ctx, Queue<byte[]> packets,
        ServerSession serverSession) {
        super(ServerDecoderManager.class.getName(), ctx, TaskPriority.ABOVE);
        this.packets = packets;
        this.serverSession = serverSession;
        this.receiveTime = System.currentTimeMillis();
    }

    protected void manage() {
        if (serverSession.getRole() == DBRole.GRAY) {
            if (checkGrayResult()) {
                setRecvTime(ctx.sqlSessionContextUtil.getGrayQueryStatistics());
                new EventSessionTask(ctx, packets).execute();
            }
        } else {
            if (checkStatus()) {
                setRecvTime(ctx.sqlSessionContextUtil.getCurrentQueryStatistics());
                new EventSessionTask(ctx, packets).execute();
            }
        }
    }

    private boolean checkGrayResult() {
        if (!ctx.getStatus().equals(SESSION_STATUS.GRAY_RESULT)) {
            logger.warn("Receive packet from gray server at state " + ctx.getStatus() + " " + ctx
                .getClientInfo() + serverSession);
            ctx.grayUp.clearGrayUp();
            return false;
        }
        return true;
    }

    private boolean checkStatus() {
        if (packets.isEmpty()) {
            return false;
        }
        if (ctx.getStatus().equals(SESSION_STATUS.QUERY_ANALYZE) || ctx.getStatus()
            .equals(SESSION_STATUS.QUERY_HANDLE)) {
            logger.error("SqlCtx({}) received packet: {} from server at state({}) {} {}",
                ctx.hashCode(), dumpPackets(packets), ctx.getStatus(), ctx.getClientInfo(),
                serverSession);
            String message = "db connection abort";
            try {
                ctx.quitTracer.reportQuit(SessionQuitTracer.QuitTrace.ServerUnsync);
                message = String
                    .format("db connection abort [%s]", serverSession.getActiveQulifiedDbId());
            } finally {
                ctx.kill(ErrorCode.ABORT_SERVER_UNSYNC, message);
            }
            return false;
        }
        return true;
    }

    private String dumpPackets(Queue<byte[]> packets) {
        ArrayDeque<byte[]> dumper = new ArrayDeque<>(packets);
        StringBuilder sb = new StringBuilder();
        while (!dumper.isEmpty()) {
            sb.append(Arrays.toString(dumper.poll()));
        }
        return sb.toString();
    }

    private void setRecvTime(QueryStatistics queryStatistics) {
        if (queryStatistics.getsRecvTime() <= queryStatistics.getsSendTime()) {
            queryStatistics.setsRecvTime(receiveTime);
        }
    }
}
