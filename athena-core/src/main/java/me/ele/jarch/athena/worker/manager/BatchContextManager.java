package me.ele.jarch.athena.worker.manager;

import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.netty.BatchSessionContext;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.netty.state.SESSION_STATUS;
import me.ele.jarch.athena.server.pool.BatchOperateServerSession;
import me.ele.jarch.athena.server.pool.ServerSessionErrorHandler;
import me.ele.jarch.athena.sharding.ShardingRouter;
import me.ele.jarch.athena.worker.SchedulerWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

public class BatchContextManager extends Manager {
    private static Logger logger = LoggerFactory.getLogger(BatchContextManager.class);
    private BatchSessionContext batchSessionContext = null;
    private BatchOperateServerSession serverSession = null;
    private ServerSessionErrorHandler errorHandler = null;
    private AtomicBoolean released = new AtomicBoolean(false);

    public BatchContextManager(BatchOperateServerSession serverSession, SqlSessionContext ctx) {
        super(BatchContextManager.class.getName(), ctx);
        this.serverSession = Objects.requireNonNull(serverSession,
            () -> "BatchOperateServerSession must be not null, msg: " + toString());
        this.batchSessionContext = new BatchSessionContext(this);
    }

    @Override protected void manage() {
        try {
            this.batchSessionContext.execute();
        } catch (QuitException e) {
            logger.error("error when BatchContextManager.manage()", e);
            this.batchSessionContext.closeUserConn();
        }
    }

    public void setPacketAndEnqueue(byte[] packet, ServerSessionErrorHandler errorHandler) {
        this.errorHandler = errorHandler;
        this.batchSessionContext.setPacket(packet);
        SchedulerWorker.getInstance().enqueue(this);
    }

    public void release() {
        if (released.getAndSet(true)) {
            return;
        }
        this.serverSession = null;
        this.ctx.batchCond.setNeedBatchAnalyze(false);
        this.batchSessionContext.enqueue(SESSION_STATUS.BATCH_QUIT);
    }

    public void sendResult2User(Queue<byte[]> packets) {
        if (packets == null) {
            return;
        }
        SchedulerWorker.getInstance()
            .enqueue(new ServerDecoderManager(this.ctx, packets, this.serverSession));
    }

    public SqlSessionContext getSqlSessionContext() {
        return this.ctx;
    }

    public String getGroupName() {
        return this.ctx.getHolder().getDalGroup().getName();
    }

    public boolean isAutoCommit4Client() {
        return this.ctx.transactionController.isAutoCommit();
    }

    public boolean isBindMaster() {
        return this.ctx.bind2master;
    }

    public ShardingRouter getShardingRouter() {
        return this.ctx.getShardingRouter();
    }

    public long getConnectionId() {
        return this.ctx.connectionId;
    }

    public String getTransactionId() {
        return this.ctx.transactionId;
    }

    public String etraceCurrentRequestId() {
        return ctx.sqlSessionContextUtil.getDalEtraceProducer().currentRequestId();
    }

    public String etraceNextLocalRpcId() {
        return ctx.sqlSessionContextUtil.getDalEtraceProducer().nextLocalRpcId();
    }

    public void closeUserConn() {
        this.errorHandler.invoke(this.serverSession);
    }
}
