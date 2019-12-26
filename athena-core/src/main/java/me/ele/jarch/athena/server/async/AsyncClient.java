package me.ele.jarch.athena.server.async;

import com.github.mpjct.jmpjct.mysql.proto.Com_Query;
import com.github.mpjct.jmpjct.mysql.proto.Com_Quit;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.util.concurrent.Future;
import me.ele.jarch.athena.allinone.DBConnectionInfo;
import me.ele.jarch.athena.exception.AuthQuitException;
import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.netty.AthenaEventLoopGroupCenter;
import me.ele.jarch.athena.server.pool.ServerSession.SERVER_SESSION_STATUS;
import me.ele.jarch.athena.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public abstract class AsyncClient {
    private static Logger logger = LoggerFactory.getLogger(AsyncClient.class);

    protected final long timeoutMillis;

    protected volatile Channel serverChannel;
    public final AsyncSqlServerPacketDecoder sqlServerPackedDecoder =
        newAsyncSqlServerPacketDecoder();

    private static List<SERVER_SESSION_STATUS> CONNECTED_STATUS = Arrays
        .asList(SERVER_SESSION_STATUS.QUERY, SERVER_SESSION_STATUS.QUERY_RESULT,
            SERVER_SESSION_STATUS.QUIT);

    protected final DBConnectionInfo dbConnInfo;

    protected Queue<byte[]> serverPackets;
    protected final AtomicBoolean isAvailable = new AtomicBoolean(true);

    protected final long birthday = System.currentTimeMillis();
    protected volatile long connectionId = 0l;

    public AsyncClient(DBConnectionInfo dbConnInfo) {
        this(dbConnInfo, 3000);
    }

    public AsyncClient(DBConnectionInfo dbConnInfo, long timeoutMilli) {
        this.dbConnInfo = dbConnInfo;
        this.timeoutMillis = timeoutMilli;
    }

    protected AsyncSqlServerPacketDecoder newAsyncSqlServerPacketDecoder() {
        return new AsyncMySqlServerPacketDecoder();
    }

    public void setServerPackets(Queue<byte[]> packets) {
        this.serverPackets = packets;
    }

    private volatile SERVER_SESSION_STATUS status = SERVER_SESSION_STATUS.INIT;

    protected SERVER_SESSION_STATUS getStatus() {
        return status;
    }

    public synchronized void setStatus(SERVER_SESSION_STATUS status) {
        if (this.status == SERVER_SESSION_STATUS.QUIT) {
            return;
        }
        this.status = status;
    }

    protected void doInit() throws QuitException {
        setStatus(SERVER_SESSION_STATUS.HANDSHAKE_AUTH);
        Bootstrap b = new Bootstrap();
        b.group(AthenaEventLoopGroupCenter.getAsyncWorkerGroup());
        b.channel(AthenaEventLoopGroupCenter.getChannelClass());
        b.option(ChannelOption.SO_KEEPALIVE, true);
        b.handler(new ChannelInitializer<Channel>() {
            @Override protected void initChannel(Channel ch) throws Exception {
                AsyncClient.this.serverChannel = ch;
                AsyncClient.this.sqlServerPackedDecoder.setServerSession(AsyncClient.this);
                ch.pipeline().addLast(AsyncClient.this.sqlServerPackedDecoder);
            }
        });

        if (logger.isDebugEnabled()) {
            logger.debug(String
                .format("connecting to mysql : %s:%d", dbConnInfo.getHost(), dbConnInfo.getPort()));
        }

        b.connect(dbConnInfo.getHost(), dbConnInfo.getPort())
            .addListener(new ChannelFutureListener() {

                @Override public void operationComplete(ChannelFuture future) throws Exception {
                    if (!future.isSuccess()) {
                        doQuit(String.format("mysql db server connection failed,server: [%s:%d]",
                            AsyncClient.this.dbConnInfo.getHost(),
                            AsyncClient.this.dbConnInfo.getPort()));
                    } else {
                        if (logger.isDebugEnabled()) {
                            logger.debug(String
                                .format("connect to mysql : %s:%d finished", dbConnInfo.getHost(),
                                    dbConnInfo.getPort()));
                        }

                    }
                }
            });

        // 检测连接超时，超时断连接，并设置 isAvailable.set(false)。 心跳剔除不可用节点
        timer.schedule(() -> {
            if (AsyncClient.this.serverChannel == null || !CONNECTED_STATUS.contains(this.status)) {
                doQuit("Failed to connect to the port of db server in " + timeoutMillis
                    + " milliseconds");
            }
        }, timeoutMillis, TimeUnit.MILLISECONDS);
    }

    public DBConnectionInfo getDbConnInfo() {
        return dbConnInfo;
    }

    protected static final ScheduledExecutorService timer =
        Executors.newSingleThreadScheduledExecutor();

    public void dbServerWriteAndFlush(byte[] buf) {
        dbServerWriteAndFlush(buf, (future) -> {
            if (!future.isSuccess()) {
                doQuit("send to mysql error.");
            }
        });
    }

    @SuppressWarnings("rawtypes")
    public void dbServerWriteAndFlush(byte[] buf, Consumer<Future> consumer) {
        Channel tmp = serverChannel;
        if (tmp == null) {
            return;
        }
        tmp.writeAndFlush(Unpooled.wrappedBuffer(buf))
            .addListener(future -> consumer.accept(future));
    }

    protected void doHandshakeAndAuth() throws QuitException {
        byte[] packet = serverPackets.poll();
        if (packet == null) {
            return;
        }
        // 到这一步的时候tcp连接已经建立，MySQL会主动向dal发送握手
        CmdHandshakeReal cmdHandshake = new CmdHandshakeReal(packet);
        cmdHandshake.execute();
        this.connectionId = cmdHandshake.handshake.connectionId;
        // dal->db 这里的dbConnInfo中的passwd已经是明文，所以这里dal向MySQL发送的auth包不会有问题
        CmdAuthReal auth = new CmdAuthReal(this.dbConnInfo, cmdHandshake.handshake);
        auth.execute();
        setStatus(SERVER_SESSION_STATUS.AUTH_RESULT_SETAUTOCOMMIT);
        dbServerWriteAndFlush(auth.getSendBuf());
    }

    protected void sendQueryToServer(String query) {
        Com_Query cmdQuery = new Com_Query();
        cmdQuery.setQuery(query);
        dbServerWriteAndFlush(cmdQuery.toPacket());
    }

    private void closeServerChannelGracefully() {
        Channel tmp = serverChannel;
        if (Objects.isNull(tmp)) {
            return;
        }
        tmp.writeAndFlush(Unpooled.wrappedBuffer(newQuitPackets()))
            .addListener(future -> tmp.close());
    }

    protected byte[] newQuitPackets() {
        return Com_Quit.getQuitPacketBytes();
    }

    protected void doAuthResult() throws QuitException {
        byte[] packet = serverPackets.poll();
        if (packet == null) {
            return;
        }
        CmdAuthResultReal authResult = new CmdAuthResultReal(packet);
        authResult.execute();
        setStatus(SERVER_SESSION_STATUS.RECEIVE_LAST_OK);
        sendQueryToServer("SET AUTOCOMMIT = 0");
    }

    private void doRecieveLastOK() throws QuitException {
        byte[] packet = serverPackets.poll();
        if (packet == null) {
            return;
        }
        setStatus(SERVER_SESSION_STATUS.QUERY);
        this.execute();
    }

    protected volatile CmdQueryResult result = null;

    // shouldn't call get() after cancel it
    protected ScheduledFuture task = null;

    protected synchronized void cancelTask() {
        if (task == null) {
            return;
        }
        task.cancel(false);
    }

    protected void doQuery() {
        setStatus(SERVER_SESSION_STATUS.QUERY_RESULT);
        task = timer.schedule(() -> {
            try {
                handleTimeout();
            } catch (Exception e) {
                logger.error("exception in handleTimeout()", e);
                doQuit("exception in handleTimeout()");
            }
        }, timeoutMillis, TimeUnit.MILLISECONDS);
        String query = getQuery();
        sendQueryToServer(query);
    }

    protected abstract String getQuery();

    protected abstract void handleResultSet(ResultSet resultSet);

    protected abstract void handleTimeout();

    protected abstract void handleChannelInactive();

    protected void doResult() throws QuitException {
        if (result == null) {
            QueryResultContext ctx = new AsyncQueryResultContext();
            ctx.packets = serverPackets;
            result = new CmdQueryResult(ctx);
        }
        result.execute();
        if (result.isDone()) {
            cancelTask();
            this.isAvailable.set(true);
            setStatus(SERVER_SESSION_STATUS.QUERY);
            AsyncQueryResultContext ctx = (AsyncQueryResultContext) result.getCtx();
            this.result = null;
            handleResultSet(new ResultSet(ctx.responseClassifier, ctx.resultType, ctx.err, ctx.ok));
        }
    }

    public boolean doAsyncExecute() {
        if (!this.isAvailable.getAndSet(false)) {
            return false;
        }
        if (status == SERVER_SESSION_STATUS.QUERY) {
            execute();
            return true;
        } else if (status == SERVER_SESSION_STATUS.INIT) {
            execute();
            return true;
        }
        return false;
    }

    private long init_time;
    private long handshake_auth_time;
    private long auth_result_time;
    private long querytime;

    synchronized protected void execute() {
        try {
            switch (status) {
                case INIT:
                    init_time = init_time == 0 ? System.currentTimeMillis() : init_time;
                    doInit();
                    break;
                case HANDSHAKE_AUTH:
                    handshake_auth_time =
                        handshake_auth_time == 0 ? System.currentTimeMillis() : handshake_auth_time;
                    doHandshakeAndAuth();
                    break;
                case AUTH_RESULT_SETAUTOCOMMIT:
                    auth_result_time =
                        auth_result_time == 0 ? System.currentTimeMillis() : auth_result_time;
                    doAuthResult();
                    break;
                case RECEIVE_LAST_OK:
                    doRecieveLastOK();
                    querytime = querytime == 0 ? System.currentTimeMillis() : querytime;
                    if (logger.isDebugEnabled()) {
                        logger.debug(
                            "AsyncClient is ready to send query, time in millis: " + (querytime
                                - init_time));
                    }
                    break;
                case QUERY:
                    doQuery();
                    break;
                case QUERY_RESULT:
                    doResult();
                    break;
                case QUIT:
                    doQuit("");
                    break;
                default:
                    logger.error("Unknown status! " + status);
            }
        } catch (AuthQuitException e) {
            logger.error("AuthQuitException in AsyncClient.execute()", e);
            setStatus(SERVER_SESSION_STATUS.QUIT);
            doQuit(String
                .format("Failed to authenticate to mysql db , message: [%s]", e.getMessage()));
        } catch (Exception t) {
            logger.error(Objects.toString(t));
            setStatus(SERVER_SESSION_STATUS.QUIT);
            doQuit(t.getMessage());
        }
    }

    public synchronized void doQuit(String reason) {
        if (!"".equals(reason)) {
            logger.error("asyncConnId={}, doQuit: {}", connectionId, reason);
        }
        this.isAvailable.set(false);
        setStatus(SERVER_SESSION_STATUS.QUIT);
        closeServerChannelGracefully();
        cancelTask();
    }

    public boolean isAvailable() {
        return isAvailable.get();
    }

    public long getBirthday() {
        return birthday;
    }

    public long getConnectionId() {
        return connectionId;
    }
}
