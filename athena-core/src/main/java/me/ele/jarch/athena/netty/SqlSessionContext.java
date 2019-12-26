package me.ele.jarch.athena.netty;

import com.github.mpjct.jmpjct.mysql.proto.ERR;
import com.github.mpjct.jmpjct.mysql.proto.Flags;
import com.github.mpjct.jmpjct.mysql.proto.Handshake;
import com.github.mpjct.jmpjct.mysql.proto.OK;
import com.github.mpjct.jmpjct.util.Authenticator;
import com.github.mpjct.jmpjct.util.ErrorCode;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import me.ele.jarch.athena.AutoreadSwitch;
import me.ele.jarch.athena.allinone.Footprint;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.constant.Metrics;
import me.ele.jarch.athena.constant.TraceNames;
import me.ele.jarch.athena.exception.AuthQuitException;
import me.ele.jarch.athena.exception.QueryException;
import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.netty.SessionQuitTracer.QuitTrace;
import me.ele.jarch.athena.netty.state.*;
import me.ele.jarch.athena.scheduler.DBChannelDispatcher;
import me.ele.jarch.athena.scheduler.EmptyScheduler;
import me.ele.jarch.athena.scheduler.Scheduler;
import me.ele.jarch.athena.server.pool.ServerSession;
import me.ele.jarch.athena.server.pool.ServerSession.AutotreadHandler;
import me.ele.jarch.athena.server.pool.ServerSession.SERVER_SESSION_STATUS;
import me.ele.jarch.athena.server.pool.ServerSessionErrorHandler;
import me.ele.jarch.athena.sharding.GrayUp;
import me.ele.jarch.athena.sharding.ShardingRouter;
import me.ele.jarch.athena.sharding.sql.IterableShardingResult;
import me.ele.jarch.athena.sharding.sql.Send2BatchCond;
import me.ele.jarch.athena.sharding.sql.SimpleIterableShardingResult;
import me.ele.jarch.athena.sql.CmdQuery;
import me.ele.jarch.athena.sql.ComposableCommand;
import me.ele.jarch.athena.sql.QUERY_TYPE;
import me.ele.jarch.athena.sql.QueryResultContext;
import me.ele.jarch.athena.sql.rscache.ResultPacketsCache;
import me.ele.jarch.athena.sql.seqs.SeqsParse;
import me.ele.jarch.athena.util.*;
import me.ele.jarch.athena.util.MulSemaphore.DALPermits;
import me.ele.jarch.athena.util.ResponseStatus.ResponseType;
import me.ele.jarch.athena.util.etrace.MetricFactory;
import me.ele.jarch.athena.util.rmq.RmqSqlInfo;
import me.ele.jarch.athena.worker.SchedulerWorker;
import me.ele.jarch.athena.worker.manager.CallBackManager;
import me.ele.jarch.athena.worker.manager.CallBackManager.CallBackStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class SqlSessionContext extends ComposableCommand implements Comparable<SqlSessionContext> {
    private static final Logger logger = LoggerFactory.getLogger(SqlSessionContext.class);
    public boolean bind2master;
    private final String clientInfo;
    public final TransactionController transactionController;
    public Channel clientChannel;
    public Queue<byte[]> clientPackets;
    public Queue<byte[]> dbServerPackets;
    // bytes counts write to client
    public long writeToClientCounts = 0L;
    // 当前从db 写到dal的数据量,sharding模式下和写到client的数据量相同
    // sharding模式下,代表当前sharding分片下的数据量
    public long currentWriteToDalCounts = 0L;
    public RmqSqlInfo rmqSqlInfo;
    public final Authenticator authenticator = newAuthenticator();

    protected Authenticator newAuthenticator() {
        return new Authenticator();
    }

    public SeqsParse seqsParse;

    public final SessionQuitTracer quitTracer = new SessionQuitTracer(this);

    private volatile DBChannelDispatcher holder = null;
    public final SqlClientPacketDecoder sqlClientPacketDecoder;
    public boolean isJDBCPrepareStmt = false;
    public final GrayUp grayUp = new GrayUp(this);

    public Scheduler scheduler = EmptyScheduler.emptySched;
    private String transDbid = "";
    public ServerSessionErrorHandler defaultErrorHandler = (serverSession) -> {
        String message = "db connection abort";
        try {
            logger.error("dbServerWriteAndFlush failed, close channel, curQuery=" + getCurQuery());
            quitTracer.reportQuit(QuitTrace.Send2ServerBroken);
            message =
                String.format("db connection abort [%s]", serverSession.getActiveQulifiedDbId());
        } finally {
            kill(ErrorCode.ABORT_SERVER_BROKEN, message);
        }
    };
    public final int workerIdx;
    /**
     * 在需要时缓存当前sql的结果
     */
    public Optional<ResultPacketsCache> resultPacketsCache = Optional.empty();
    private ClientWriteBuf clientWriteBuf = new ClientWriteBuf();

    public SqlSessionContext(SqlClientPacketDecoder sqlClientPacketDecoder, String clientAddr) {
        this(sqlClientPacketDecoder, false, clientAddr);
    }

    public SqlSessionContext(SqlClientPacketDecoder sqlClientPacketDecoder, boolean bind2master,
        String clientAddr) {
        this.clientAddr = clientAddr;
        this.sqlClientPacketDecoder = sqlClientPacketDecoder;
        this.bind2master = bind2master;
        initState();
        long hashCode = this.hashCode();
        this.workerIdx = (int) (Math.abs(hashCode) % SchedulerWorker.getPoolSize());
        this.clientInfo = "clientConnId=" + connectionId + " " + clientAddr;
        this.transactionController = newTransactionController();
    }

    private void initState() {
        // specific logic for PG/MySQL
        this.clientFakeHandshakeState = newClientFakeHandshakeState();
        this.clientFakeAuthState = newClientFakeAuthState();
        this.queryAnalyzeState = newQueryAnalyzeState();
        this.queryHandleState = newQueryHandleState();
        this.queryResultState = newQueryResultState();
        this.fakeSqlState = newFakeSqlState();
        // common logic
        this.fieldsResultState = new FieldsResultState(this);
        this.grayQueryHandleState = new GrayQueryHandleState(this);
        this.grayState = new GrayState(this);
        this.quitState = new QuitState(this);

        this.state = this.clientFakeHandshakeState;
    }

    protected TransactionController newTransactionController() {
        return new MySqlTransactionController();
    }

    protected ClientFakeHandshakeState newClientFakeHandshakeState() {
        return new ClientFakeHandshakeState(this);
    }

    protected ClientFakeAuthState newClientFakeAuthState() {
        return new ClientFakeAuthState(this);
    }

    protected QueryAnalyzeState newQueryAnalyzeState() {
        return new QueryAnalyzeState(this);
    }

    protected QueryHandleState newQueryHandleState() {
        return new QueryHandleState(this);
    }

    protected QueryResultState newQueryResultState() {
        return new QueryResultState(this);
    }

    protected FakeSqlState newFakeSqlState() {
        return new FakeSqlState(this);
    }

    public void setDBChannelDispatcher(DBChannelDispatcher dbChannelDispatcher) {
        setHolder(dbChannelDispatcher);
        this.grayUp.setClientAddr(this.getClientAddr());
    }

    public DBChannelDispatcher getHolder() {
        return holder;
    }

    private final String clientAddr;

    public void setHolder(DBChannelDispatcher holder) {
        this.holder = holder;
        this.batchCond.setDalGroupNameAndInitBatchAllowed(
            this.holder != null && this.holder.getDalGroup() != null ?
                this.holder.getDalGroup().getName() :
                "");
    }

    public String getClientAddr() {
        return clientAddr;
    }

    public String getClientInfo() {
        return clientInfo;
    }

    public final Send2BatchCond batchCond = new Send2BatchCond();

    public final Send2BatchCond cmdQueryBatchCond = new Send2BatchCond();

    public Send2BatchCond resetNeedBatchAnalyzeAfterShard(CmdQuery cmdQuery) {
        if (cmdQuery.getShardingRouter() == ShardingRouter.defaultShardingRouter) {
            /*
             * When this is non-sharding environment, we keep the batchCond as its defaults.
             */
        } else if (batchCond.isBatchAllowed() && !batchCond.isNeedBatchAnalyze()) {
            /*@formatter:off
             * We only try to set needBatchAnalyze state when it is false.
             * Because in a transaction, (set autocommit=0; insert xxxx; select xxxx; commit;),
             * this state can be set to true by the first `insert`; when the second `select` comes in,
             * the transaction has not ended, we can not set it back to false;
             * @formatter:on
             */
            batchCond.setNeedBatchAnalyze(cmdQuery.batchCond.isNeedBatchAnalyze());
        }
        return batchCond;
    }

    public volatile CmdQuery curCmdQuery = null;

    // SQL在dal内部的路由选择，默认是 UNKNOWN_FOOTPRINT
    public Footprint curFootprint = Footprint.UNKNOWN_FOOTPRINT;

    // whether session is in transaction status
    private volatile boolean inTransStatus = false;

    public volatile boolean isInShardedMode = false;
    // whether session is execute mapping sql
    public volatile boolean isInMappingShardedMode = false;
    public volatile int shardingCount = 0;
    public volatile String shardingId = "";
    public IterableShardingResult shardingResult = new SimpleIterableShardingResult();
    public ShardingContext shardingContext;

    public void initSharingContext(IterableShardingResult results) {
        shardingResult = results;
        shardingResult.aggrMethods.forEach(func -> validateAggrFunc(func));
        shardingContext = newShardingContext(quitTracer);
        shardingContext.init(shardingResult, curCmdQuery.originalTable, curCmdQuery.queryType);
    }

    public ShardingContext newShardingContext(SessionQuitTracer quitTracer) {
        return new ShardingContext(quitTracer);
    }

    private void validateAggrFunc(AggregateFunc func) throws QueryException {
        if (!func.validate()) {
            String errorMessage = String.format(Constants.NOT_SUPPORT_FUNCTION, func);
            throw new QueryException.Builder(ErrorCode.ER_COLUMNACCESS_DENIED_ERROR)
                .setSequenceId(1).setErrorMessage(errorMessage).bulid();
        }
    }

    public Map<String, ServerSession> shardedSession = new LinkedHashMap<>();

    public boolean isInTransStatus() {
        return inTransStatus;
    }

    public void setTransStatus(boolean status) {
        inTransStatus = status;
        if (status) {
            this.sqlSessionContextUtil.startTransStatistics(transactionId, curCmdQuery.rid);
            setContextBeginTran();
        } else {
            transactionController.onTransactionEnd();
            this.sqlSessionContextUtil.getTransStatistics().settEndTime(System.currentTimeMillis());
            setContextEndTran();
            setTransDbid("");
            writeCommitMetrics(transactionId);
        }
    }

    private void setContextBeginTran() {
        if (isInShardedMode && shardingResult != null) {
            shardingCount = shardingResult.count;
        }
    }

    private void setContextEndTran() {
        shardingCount = 0;
        shardingId = "";
    }

    private void writeCommitMetrics(String tmpTransId) {
        long commitDuration = this.sqlSessionContextUtil.getTransStatistics().getTransationDur();
        MetricFactory.newCounterWithSqlSessionContext(Metrics.DURATION_COMMIT, this)
            .value(commitDuration);
        transLog(tmpTransId, 0);
        this.sqlSessionContextUtil.resetTransTimeRecord();
    }

    /**
     * 事务执行时间日志
     *
     * @param transId
     *            事务ID
     * @param logType
     *            日志类型: 0--info,1--error
     */
    public void transLog(String transId, int logType) {
        StringBuilder builder = new StringBuilder(512);
        TransStatistics ts = this.sqlSessionContextUtil.getTransStatistics();
        long transDur = ts.getTransationDur();
        builder.append(getClientInfo()).append(" => ")
            .append(this.sqlSessionContextUtil.getResponseType()).append(" => transId=")
            .append(transId).append(" => transDur:").append(transDur);
        builder.append(ts.getTransTime()).append(" => ");
        String sql = this.getCurQuery();
        switch (logType) {
            case 0:
                sqlSessionContextUtil.endSqlTransactionProducer(io.etrace.common.Constants.SUCCESS);
                builder.append("tEnd_").append(sql);
                logger.info(builder.toString());
                break;
            case 1:
                sqlSessionContextUtil.endSqlTransactionProducer(io.etrace.common.Constants.FAILURE);
                builder.append("[").append(sql).append("] => ");
                builder.append("tEnd_abort");
                logger.error(builder.toString());
                break;
            default:
                builder.append("tEnd_").append(sql);
                logger.info(builder.toString());
        }
    }

    public QUERY_TYPE getQueryType() {
        CmdQuery curCmdQuery = this.curCmdQuery;
        return curCmdQuery == null ? QUERY_TYPE.OTHER : curCmdQuery.queryType;
    }

    public QUERY_TYPE getCurQueryType() {
        CmdQuery curCmdQuery = this.curCmdQuery;
        return curCmdQuery == null ? QUERY_TYPE.OTHER : curCmdQuery.curQueryType;
    }

    public String getCurQuery() {
        CmdQuery curCmdQuery = this.curCmdQuery;
        return curCmdQuery == null ? "" : curCmdQuery.query;
    }

    public volatile long queryId = -1;

    public volatile long commitId = -1;

    public volatile boolean longLivedSession = false;

    public volatile String transactionId = Constants.DEFAULT_TRANS_ID;

    public final long connectionId = Handshake.IDGENERATOR.getId();

    public final UUID uuid = UUID.randomUUID();

    @Override public int compareTo(SqlSessionContext o) {
        return this.uuid.compareTo(o.uuid);
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        SqlSessionContext that = (SqlSessionContext) o;

        return uuid != null ? uuid.equals(that.uuid) : that.uuid == null;

    }

    @Override public int hashCode() {
        return uuid != null ? uuid.hashCode() : 0;
    }

    AtomicReference<DALPermits> semRef = new AtomicReference<>();
    AtomicReference<DALPermits> trxsemRef = new AtomicReference<>();

    public final SqlSessionContextUtil sqlSessionContextUtil = new SqlSessionContextUtil(this);

    public SESSION_STATUS getStatus() {
        return state.getStatus();
    }

    public Channel getClientChannel() {
        return clientChannel;
    }

    public void setClientChannel(Channel clientChannel) {
        this.clientChannel = clientChannel;
    }

    // 绑定网络数据包给相应的SqlSessionContext
    // 该方法实现MySQL是赋值
    // PG实现是追加(因为PG支持在没收完一个结果集的情况下发送多个SQL包)
    public void bindClientPackets(Queue<byte[]> clientPackets) {
        this.clientPackets = clientPackets;
    }

    /**
     * 用于非sharding以及sharding结束时打印origin sharded sql session信息
     * @return
     */
    public String sessInfo() {
        try {
            StringBuilder sb = new StringBuilder(512);

            printSessionPrefix(sb);

            if (!shardedSession.isEmpty()) {
                for (Map.Entry<String, ServerSession> entry : shardedSession.entrySet()) {
                    entry.getValue().print(sb);
                }
            }
            return sb.toString();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return "Corrupted server session";
        }
    }

    /**
     * 用于sharding场景中打印某跳分片后sql对应的session信息
     * @return
     */
    public String currentSessInfo() {
        try {
            StringBuilder sb = new StringBuilder(512);
            printSessionPrefix(sb);
            sb.append(shardedSession.get(this.scheduler.getInfo().getGroup()));
            return sb.toString();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return "Corrupted server session";
        }
    }

    /**
     * 用于ignore cmd或者dal_dual等被dal拦截掉的sql,生成日志中的上下信息,不包含数据库信息。
     * @return
     */
    public String fixedSessInfo() {
        try {
            StringBuilder sb = new StringBuilder(128);
            printSessionPrefix(sb);
            return sb.toString();
        } catch (Exception e) {
            logger.error("error when generator client info", e);
            return "Corrupted fixed sess info";
        }
    }

    private void printSessionPrefix(StringBuilder sb) {
        if (authenticator != null) {
            sb.append(authenticator.userName);
            if (logger.isDebugEnabled()) {
                sb.append("(readonly=").append(authenticator.isReadOnly()).append(")");
            }
        }

        DBChannelDispatcher holder = this.holder;
        if (holder != null) {
            sb.append("@").append(holder.getCfg());
        }

        sb.append(" transId=").append(transactionId).append(" ");
        if (this.isInTransStatus()) {
            sb.append(this.sqlSessionContextUtil.getTransTime()).append(" ");
        }
    }

    @Override public String toString() {
        StringBuilder sb = new StringBuilder("clientConnId=").append(connectionId).append(" ");
        sb.append(clientAddr).append(" ").append(sqlSessionContextUtil.getPacketTime()).append(" ")
            .append(state.getStatus()).append(" ");
        sb.append(getCurQuery()).append(" ").append(sessInfo());
        return sb.toString();
    }

    public void setDbServerPackets(Queue<byte[]> dbServerPackets) {
        this.dbServerPackets = dbServerPackets;
    }

    private void clearChannel() {
        try {
            closeClientChannel();
            this.grayUp.clearGrayUp();
        } finally {
            closeServerSession();
        }
    }

    protected ByteBuf buildKillErr(long errorNo, String sqlState, String dalMessage,
        long sequenceId) {
        return Unpooled.wrappedBuffer(ERR.buildErr(sequenceId, errorNo, dalMessage).toPacket());
    }

    private void internalKill(long errorNo, String sqlState, String message, long sequenceId) {
        try {
            NoThrow.call(() -> {
                String user = "anonymous";
                Authenticator authenticator = this.authenticator;
                if (authenticator != null && this.authenticator.userName != null
                    && !this.authenticator.userName.isEmpty()) {
                    user = this.authenticator.userName;
                }
                String dalMessage = String
                    .format("Aborted connection %d to db: '%s' user: '%s' (%s)", this.connectionId,
                        this.getHolder().getCfg(), user, message);
                boolean isQueryReceived = !this.getCurQuery().isEmpty();
                if (AthenaConfig.getInstance().isSendAbort() && isQueryReceived) {
                    clientWriteAndFlushQuit(
                        buildKillErr(errorNo, sqlState, dalMessage, sequenceId));
                } else {
                    doQuit();
                }
                logger.error(
                    String.format("current context: %s, msg: %s", this.toString(), dalMessage));
            });
        } finally {
            setState(SESSION_STATUS.QUIT);
            doQuit();
        }
    }

    public void kill(ErrorCode errorCode, String message) {
        kill(errorCode, message, 1);
    }

    public void kill(ErrorCode errorCode, String message, long sequenceId) {
        internalKill(errorCode.getErrorNo(), errorCode.getSqlState(), message, sequenceId);
    }

    /**
     * This method will be invoked by the clientChannel#writeHandler after flushing all the sql response to client.
     */
    public void releaseDbSemaphore() {
        DALPermits sem = semRef.getAndSet(null);
        if (sem != null) {
            sem.release();
            scheduler = EmptyScheduler.emptySched;
        }
    }

    public void setDbSemaphore(DALPermits activeDBSessionsSemaphore) {
        DALPermits oldSem = semRef.getAndSet(activeDBSessionsSemaphore);

        if (oldSem != null) {
            oldSem.release();
        }
    }

    public void releaseTrxSemaphore() {
        DALPermits trxsem = trxsemRef.getAndSet(null);
        if (trxsem != null) {
            trxsem.release();
        }
    }

    public void setTrxSemaphore(DALPermits activeDBSessionsSemaphore) {
        DALPermits oldSem = trxsemRef.getAndSet(activeDBSessionsSemaphore);
        if (oldSem != null) {
            oldSem.release();
        }
    }

    public void closeClientChannel() {
        try {
            if (clientChannel != null) {
                clientChannel.close();
                clientChannel = null;
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public void returnServerSession(ServerSession idleSession) throws QuitException {
        try {
            if (idleSession != null) {
                this.shardedSession.remove(idleSession.getActiveGroup(), idleSession);
                idleSession.returnServerSession();
            }
        } catch (Exception e) {
            logger.error("Failed to return server session" + idleSession.toString(), e);
        }
    }

    public void returnServerSession() throws QuitException {
        Map<String, ServerSession> tmpMap = this.shardedSession;
        this.shardedSession = new LinkedHashMap<String, ServerSession>();
        tmpMap.forEach((dbGroupName, tmpSession) -> {
            // It should not be null
            if (tmpSession == null) {
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format(
                        "Server session has been returned. ignore this return operation in sql session context [%s]",
                        this));
                }
            }
            try {
                tmpSession.returnServerSession();
            } catch (Exception e) {
                logger.error("Failed to return server session" + tmpSession.toString(), e);
            }
        });
        tmpMap.clear();
    }

    public void closeServerSession() {
        Map<String, ServerSession> tmpMap = this.shardedSession;
        this.shardedSession = new LinkedHashMap<String, ServerSession>();
        while (tmpMap.size() > 0) {
            tmpMap.forEach((dbGroupName, tmpSession) -> {
                // It should not be null
                if (tmpSession == null) {
                    if (logger.isDebugEnabled()) {
                        logger.debug(String.format(
                            "Server session has been returned. ignore this close operation in sql session context [%s]",
                            this));
                    }
                    return;
                }
                try {
                    tmpSession.closeServerSession();
                } catch (Exception e) {
                    logger.error("Failed to close server session: " + tmpSession.toString(), e);
                }
            });
            tmpMap.clear();
        }
    }

    private void clientWriteAndFlushQuit(ByteBuf buf) {
        try {
            setState(SESSION_STATUS.QUIT);
            Channel tmpChannel = clientChannel;
            if (tmpChannel == null) {
                doQuit();
                ReferenceCountUtil.release(buf);
                return;
            }
            this.resultPacketsCache
                .ifPresent(resultCache -> resultCache.tryAppendPacket(buf.array()));
            clientWriteBuf.write(buf);
            tmpChannel.writeAndFlush(clientWriteBuf.readall()).addListener(future -> {
                SchedulerWorker.getInstance()
                    .enqueue(new CallBackManager(this, future, CallBackStatus.CLIENT_FLUSH_QUIT));
            });
        } catch (Exception e) {
            doQuit();
            logger.error("Failed to send quit packets to client " + SqlSessionContext.this, e);
        }
    }

    public void clientWriteAndFlush(ByteBuf buf,
        GenericFutureListener<? extends Future<? super Void>>... listeners) {
        clientWriteAndFlushWithTrafficControll(buf, null, listeners);
    }

    /**
     * if channelConfig is not null, need to open/close autoread TrafficControll : 流控
     *
     * @param buf
     * @param serverSession
     *            current server session config reference
     */
    public void clientWriteAndFlushWithTrafficControll(ByteBuf buf, ServerSession serverSession,
        GenericFutureListener<? extends Future<? super Void>>... listeners) {
        Channel tmpChannel = clientChannel;
        if (tmpChannel == null) {
            ReferenceCountUtil.release(buf);
            return;
        }
        this.resultPacketsCache.ifPresent(resultCache -> resultCache.tryAppendPacket(buf.array()));
        clientWriteBuf.write(buf);
        ChannelFuture channelFuture =
            tmpChannel.writeAndFlush(clientWriteBuf.readall()).addListener(future -> {
                if (!future.isSuccess()) {
                    SchedulerWorker.getInstance()
                        .enqueue(new CallBackManager(this, future, CallBackStatus.CLIENT_FLUSH));
                }
            });
        //添加用户自定义的listener
        channelFuture.addListeners(listeners);
        if (serverSession == null) {
            return;
        }

        long currTime = System.currentTimeMillis();
        final boolean autoreadSwitch = AutoreadSwitch.autoreadOn.get();
        if (autoreadSwitch) {
            if (logger.isDebugEnabled()) {
                logger.debug(String.format(
                    "big-result-autoread trigger close [ clientConnId: %s | transId: %s | %s ]",
                    this.connectionId, this.transactionId, serverSession.toString()));
            }
            AutotreadHandler autotreadHandler = serverSession.closeAutoRead();
            channelFuture.addListener(future -> {
                long finishTime = System.currentTimeMillis() - currTime;
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format(
                        "big-result-autoread trigger open [ clientConnId: %s | transId: %s | %s | %s ms]",
                        this.connectionId, this.transactionId, serverSession.toString(),
                        finishTime));
                }
                autotreadHandler.openAutoRead();
            });
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug(
                    "big-result-autoread placeholder : clientWriteAndFlushWithTrafficControll close");
            }
            channelFuture.addListener(future -> {
                long finishTime = System.currentTimeMillis() - currTime;
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format(
                        "big-result-autoread placeholder : clientWriteAndFlushWithTrafficControll open use %s ms",
                        finishTime));
                }
            });
        }

    }

    private volatile boolean isClientLzyWriteHasFailed = false;

    public void clientLazyWrite(ByteBuf buf) {
        Channel tmpChannel = clientChannel;
        if (tmpChannel == null) {
            ReferenceCountUtil.release(buf);
            return;
        }
        this.resultPacketsCache.ifPresent(resultCache -> resultCache.tryAppendPacket(buf.array()));
        if (clientWriteBuf.write(buf) > QueryResultContext.SENT_BUFFER_SIZE) {
            tmpChannel.writeAndFlush(clientWriteBuf.readall()).addListener(future -> {
                if (!future.isSuccess()) {
                    SchedulerWorker.getInstance().enqueue(
                        new CallBackManager(this, future, CallBackStatus.CLIENT_LAZY_WRITE)
                            .setClientLazyWriteHasFailed(isClientLzyWriteHasFailed));
                }
            });
        }
    }

    public void trySaveResultPacketsCache() {
        Optional<ResultPacketsCache> tmpResultPacketsCache = resultPacketsCache;
        resultPacketsCache = Optional.empty();
        tmpResultPacketsCache.ifPresent(packetsCache -> packetsCache.trySave(scheduler));
    }

    public boolean doQuit() {
        try {
            setState(SESSION_STATUS.QUIT);
            clearChannel();
            clearEtrace();
        } finally {
            releaseDbSemaphore();
            releaseTrxSemaphore();
            sqlSessionContextUtil.getQueryStatistics().setcSendTime(System.currentTimeMillis());
        }
        return true;
    }

    private void clearEtrace() {
        sqlSessionContextUtil.endEtraceWhenQuit();
    }

    public void returnResource() throws QuitException {
        CmdQuery query = curCmdQuery;
        if (query == null) {
            logger.error("strange! CmdQuery query is null");
            return;
        }
        QUERY_TYPE type = query.queryType;
        ServerSession serverSession = this.shardedSession.get(this.scheduler.getInfo().getGroup());
        if (serverSession != null) {
            /**
             * 如果是第一条事务sql,则serverSession.isInTranscation()一定为false, 此时则serverSession的状态由queryType决定
             * 如果是事务已经开启的后续sql,serverSession.isInTranscation()一定为true, 短路后续条件。
             * 如果是autocommit的事务sql, serverSession.isInTranscation() == false, query.queryType.isWritableStatement() == true,
             * 但是this.isInTransStatus() == false。所以serverSession会在最后一归还。
             */
            serverSession.setInTranscation(
                serverSession.isInTranscation() || query.queryType.isWritableStatement());
            if (!serverSession.isInTranscation()) {
                returnServerSession(serverSession);
            } else if (shouldFinishTransaction(type)) {
                serverSession.setInTranscation(false);
            }
        }
        if (isInTransStatus()) {
            if (shouldFinishTransaction(type) && !shardingResult.hasNext()) {
                setTransStatus(false);
                returnServerSession();
            }
        } else {
            setContextEndTran();
            // 此处能够还掉autocommit的serversession
            returnServerSession();
        }
    }

    private boolean shouldFinishTransaction(QUERY_TYPE type) {
        return type.isEndTrans() || type == QUERY_TYPE.TRUNCATE;
    }

    @Override protected boolean doInnerExecute() {
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("status = %s on session [%s]", state.getStatus().toString(),
                this.toString()));
        }
        try {
            return this.state.handle();
        } catch (AuthQuitException e) {
            logger.warn(String.format("Authentication failed, close channel on [%s],message: [%s]",
                this.toString(), e));
            try {
                this.quitTracer.reportQuit(SessionQuitTracer.QuitTrace.ClientAuthenticationFailed,
                    new ResponseStatus(ResponseType.ABORT, ErrorCode.ABORT_AUTHENTICATION_FAILED,
                        e.getMessage()));
                setState(SESSION_STATUS.QUIT);
                MetricFactory.newQuitCounter(TraceNames.AUTH_FAIL, this, e.getState()).once();
            } catch (Exception exception) {
                logger.error(exception.getMessage(), exception);
            } finally {
                doQuit();
            }
            return true;
        } catch (QueryException e) {
            try {
                logger.warn(Objects.toString(e));
                this.quitTracer.reportQuit(SessionQuitTracer.QuitTrace.DALAbort,
                    new ResponseStatus(ResponseStatus.ResponseType.DALERR, e.errorCode,
                        e.errorMessage));
                this.sqlSessionContextUtil.getQueryStatistics()
                    .setcSendTime(System.currentTimeMillis());
                // 结束etrace跟踪
                this.sqlSessionContextUtil.endEtrace();
            } catch (Exception exception) {
                logger.error(exception.getMessage(), exception);
            } finally {
                kill(e.errorCode, e.errorMessage, e.sequenceId);
            }
            return true;
        } catch (Exception t) {
            try {
                this.quitTracer.reportQuit(SessionQuitTracer.QuitTrace.DALAbort,
                    new ResponseStatus(ResponseStatus.ResponseType.ABORT, ErrorCode.ABORT,
                        t.getMessage()));
                logger.error(
                    String.format("exception caught, close channel on [%s]", this.toString()), t);
                this.sqlSessionContextUtil.getQueryStatistics()
                    .setcSendTime(System.currentTimeMillis());
                // 结束etrace跟踪
                this.sqlSessionContextUtil.endEtrace();
            } catch (Exception exception) {
                logger.error(exception.getMessage(), exception);
            } finally {
                kill(ErrorCode.ABORT, "unable to connect to database");
            }
            return true;
        }
    }

    @Override public void execute() throws QuitException {
        super.execute();
        if (isDone()) {
            releaseDbSemaphore();
            releaseTrxSemaphore();
        }
    }

    public boolean attachServerSession(ServerSession session) {
        try {
            DALPermits trxsem = trxsemRef.getAndSet(null);
            session.setTrxSemaphore(trxsem);
            if (this.state == this.quitState) {
                session.setStatus(SERVER_SESSION_STATUS.QUIT);
                session.doQuit();
                return false;
            }
            ServerSession serverSession =
                this.shardedSession.put(session.getActiveGroup(), session);
            if (serverSession != null) {
                serverSession.closeServerSession();
                logger.error("Attach duplicate Server Session " + serverSession);
            }
            return true;
        } catch (Exception t) {
            session.setStatus(SERVER_SESSION_STATUS.QUIT);
            session.doQuit();
            logger.error(Objects.toString(t));
            return false;
        }
    }

    public boolean attachGrayServerSession(ServerSession session) {
        try {
            if (this.state == this.grayState && this.grayUp.isReadySend && this.grayUp
                .isStarted()) {
                this.grayUp.serverSession = session;
                return true;
            }
            if (this.curCmdQuery.isGrayRead()) {
                this.grayUp.serverSession = session;
                return true;
            }
            logger.error("ClearGrayUp has been called.");
            session.setStatus(SERVER_SESSION_STATUS.QUIT);
            session.doQuit();
            return false;
        } catch (Exception t) {
            session.setStatus(SERVER_SESSION_STATUS.QUIT);
            session.doQuit();
            logger.error(Objects.toString(t));
            return false;
        }
    }

    protected ClientFakeHandshakeState clientFakeHandshakeState;
    protected ClientFakeAuthState clientFakeAuthState;
    protected QueryAnalyzeState queryAnalyzeState;
    private FakeSqlState fakeSqlState;
    protected QueryHandleState queryHandleState;
    protected QueryResultState queryResultState;
    private FieldsResultState fieldsResultState;
    private GrayQueryHandleState grayQueryHandleState;
    private GrayState grayState;
    private QuitState quitState;
    protected volatile State state;

    public void setState(SESSION_STATUS newState) {
        if (this.state == this.quitState) {
            return;
        }
        switch (newState) {
            case CLIENT_FAKE_HANDSHAKE:
                this.state = this.clientFakeHandshakeState;
                break;
            case CLIENT_FAKE_AUTH:
                this.state = this.clientFakeAuthState;
                break;
            case QUERY_ANALYZE:
                this.state = this.queryAnalyzeState;
                break;
            case FAKE_SQL:
                this.state = this.fakeSqlState;
                break;
            case QUERY_HANDLE:
                this.state = this.queryHandleState;
                break;
            case QUERY_RESULT:
                this.state = this.queryResultState;
                break;
            case FIELDS_RESULT:
                this.state = this.fieldsResultState;
                break;
            case GRAY_QUERY_HANDLE:
                this.state = this.grayQueryHandleState;
                break;
            case GRAY_RESULT:
                this.state = this.grayState;
                break;
            case QUIT:
                if (this.state != this.quitState) {
                    this.state = this.quitState;
                }
                break;
            default:
                logger.error("Unknown status! " + newState);
        }
    }

    public String getTransDbid() {
        return transDbid;
    }

    public void setTransDbid(String transDbid) {
        this.transDbid = transDbid;
    }

    public ShardingRouter getShardingRouter() {
        if (AthenaConfig.getInstance().getGrayType() == GrayType.NOSHARDING) {
            return ShardingRouter.defaultShardingRouter;
        } else {
            return this.getHolder().getShardingRouter();
        }
    }

    /**
     * 该方法返回这个客户端是否会异步发送SQL,如:
     * <pre>
     *  JDBC --> INSERT --> PGDB
     *  JDBC --> INSERT --> PGDB
     *  JDBC <--   OK   <-- PGDB
     *  JDBC --> INSERT --> PGDB
     *  JDBC <--   OK   <-- PGDB
     *  JDBC <--   OK   <-- PGDB
     *  </pre>
     * 所以如果该方法返回true, 那么在SessionTask和ClientDecoderManager里不判断状态一致性checkStatus(),并提前返回
     * <p>
     * 这样的结果是, 如果出现上述流程图的情况,那么在第一个INSERT调用之后,状态机会一直保持在QUERY_RESULT状态
     * 此时后续任何的SessionTask和ClientDecoderManager会提前返回,不会有实际处理操作
     * <p>
     * 且由于前端多次发送INSERT, 就会多次调用bindClientPackets(),PG的实现为追加到一个队列中(MYSQL是直接赋值)
     * <p>
     * 直到第一个INSERT收完结果集之后,在QueryResultState里调用:
     * <pre>
     *      if (hasUnsyncQuery())
     *           sqlSessionContext.getHolder().enqueue(sqlSessionContext);
     *  </pre>
     * 会重新进入QueryAnalyzeState状态,并从队列(SqlSessionContext.clientPackets)里poll出一个SQL继续执行
     */
    public boolean allowUnsyncQuery() {
        return false;
    }

    /**
     * routine参数是为PG预留
     * @param errorCode
     * @param errMsg
     * @param routine
     * @return
     */
    public ByteBuf buildErr(ErrorCode errorCode, String errMsg, String routine) {
        return Unpooled.wrappedBuffer(ERR.buildErr(1, errorCode.getErrorNo(), errMsg).toPacket());
    }

    public ByteBuf buildOk() {
        OK ok = new OK();
        ok.sequenceId = 1;
        ok.setStatusFlag(transactionController.isAutoCommit() ? Flags.SERVER_STATUS_AUTOCOMMIT : 0);
        return Unpooled.wrappedBuffer(ok.toPacket());
    }

    protected boolean swallowQueryResponse = false;

    public boolean isSwallowQueryResponse() {
        return swallowQueryResponse;
    }

    public void setSwallowQueryResponse(boolean swallowQueryResponse) {
        logger.warn(
            "try set swallowQueryResponse={} which is not allowed when run in mysql protocol, will ignored",
            swallowQueryResponse);
    }
}
