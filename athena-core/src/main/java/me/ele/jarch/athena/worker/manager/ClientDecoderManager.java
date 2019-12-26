package me.ele.jarch.athena.worker.manager;

import com.github.mpjct.jmpjct.util.ErrorCode;
import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.netty.SessionQuitTracer;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.netty.state.SESSION_STATUS;
import me.ele.jarch.athena.scheduler.DBChannelDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;

public class ClientDecoderManager extends Manager {

    private static final Logger logger = LoggerFactory.getLogger(ClientDecoderManager.class);

    final Queue<byte[]> packets;
    DBChannelDispatcher holder;

    public ClientDecoderManager(SqlSessionContext ctx, Queue<byte[]> packets) {
        super(ClientDecoderManager.class.getName(), ctx);
        this.packets = packets;
    }

    public ClientDecoderManager setHolder(DBChannelDispatcher holder) {
        this.holder = holder;
        return this;
    }

    @Override protected void manage() {
        try {
            ctx.bindClientPackets(packets);
            if (holder == null) {
                bindHolder();
                return;
            }
            if (checkStatus()) {
                ctx.execute();
            }
        } catch (QuitException e) {
            ctx.doQuit();
            logger.error(
                "ClientDecoderManager quitException." + ctx.getStatus() + " " + ctx.getClientInfo(),
                e);
        }
    }

    private boolean checkStatus() {
        // if the status is QUERY_RESULT or QUERY_HANDLE while adding to SchedulerWorker,we could check error during SessionTask.execute
        if (ctx.getStatus() == SESSION_STATUS.QUERY_RESULT
            || ctx.getStatus() == SESSION_STATUS.QUERY_HANDLE) {
            if (ctx.allowUnsyncQuery()) {
                return false;
            }
            try {
                logger.error(
                    "Receive query from client at state " + ctx.getStatus() + " " + ctx.toString());
                ctx.quitTracer.reportQuit(SessionQuitTracer.QuitTrace.ClientUnsync);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            } finally {
                ctx.kill(ErrorCode.ERR_CLIENT_UNSYNC, "async query");
            }
            return false;
        }
        return true;
    }

    private void bindHolder() throws QuitException {
        ctx.execute();
    }

}
