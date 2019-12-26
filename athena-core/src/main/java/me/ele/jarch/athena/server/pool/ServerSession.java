package me.ele.jarch.athena.server.pool;

import com.github.mpjct.jmpjct.mysql.proto.Com_Query;
import com.github.mpjct.jmpjct.mysql.proto.Com_Quit;
import com.github.mpjct.jmpjct.mysql.proto.Flags;
import com.github.mpjct.jmpjct.mysql.proto.Packet;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollMode;
import io.netty.channel.epoll.EpollSocketChannel;
import me.ele.jarch.athena.allinone.DBConnectionInfo;
import me.ele.jarch.athena.allinone.DBRole;
import me.ele.jarch.athena.exception.AuthQuitException;
import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.netty.AthenaEventLoopGroupCenter;
import me.ele.jarch.athena.netty.MySqlServerPacketDecoder;
import me.ele.jarch.athena.netty.SqlServerPacketDecoder;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.sql.CmdAuthReal;
import me.ele.jarch.athena.sql.CmdAuthResultReal;
import me.ele.jarch.athena.sql.CmdHandshakeReal;
import me.ele.jarch.athena.util.MulSemaphore.DALPermits;
import me.ele.jarch.athena.worker.SchedulerWorker;
import me.ele.jarch.athena.worker.manager.CallBackManager;
import me.ele.jarch.athena.worker.manager.CallBackManager.CallBackStatus;
import me.ele.jarch.athena.worker.manager.ResponseHandlerManager;
import me.ele.jarch.athena.worker.manager.ServerSessionQuitManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ServerSession {
    private static final Logger logger = LoggerFactory.getLogger(ServerSession.class);

    private final long birthday = System.currentTimeMillis();
    protected volatile long connectionId = 0;
    protected final DBConnectionInfo dbConnInfo;
    protected final ServerSessionPool pool;
    protected volatile Channel serverChannel;
    protected final SqlServerPacketDecoder sqlServerPackedDecoder;
    private ResponseHandler handler;
    protected Queue<byte[]> serverPackets;
    protected volatile boolean isReady = false;
    private SERVER_SESSION_STATUS status = SERVER_SESSION_STATUS.INIT;
    private volatile boolean isInTranscation = false;
    private volatile boolean isNeedCloseClient = true;
    private AtomicBoolean isClosed = new AtomicBoolean(false);
    // isAutoCommit表明建立的连接是否是autocommit
    protected final boolean isAutoCommit;
    private final AtomicReference<DALPermits> trxsemRef = new AtomicReference<>();
    protected volatile String activeQulifiedDbId = "";

    //activeQulifiedDbId = activeDbGroup:activeDbId
    protected volatile String activeDbGroup = "";
    protected volatile String activeDbId = "";
    protected final Queue<String> additionSessionCfgQueue = new LinkedList<>();
    // 用于记录前一条发往数据库的SET ABC=DEF之类的命令
    private String previousAdditionalSessionCfg = "";
    protected static final ChannelInboundHandlerAdapter EMPTY_HANDLER =
        new ServerSession.EmptyChannelHandler();

    final long overdue;

    private static final long MYSQL_DEFAULT_OVERDUE_IN_MINUTES = 5;


    @ChannelHandler.Sharable static class EmptyChannelHandler extends ChannelInboundHandlerAdapter {
    }


    public final String dbFullName;

    protected ServerSession(DBConnectionInfo info, ServerSessionPool pool) {
        this.dbConnInfo = info;
        this.pool = pool;
        this.isAutoCommit = pool.isAutoCommit();
        this.activeQulifiedDbId = info.getQualifiedDbId();
        this.activeDbGroup = info.getGroup();
        this.activeDbId = info.getId();
        addAdditionalSessionCfg(info);
        sqlServerPackedDecoder = newDecoder();
        this.dbFullName = info.getHost() + ":" + info.getPort() + "/" + info.getDatabase();
        overdue = calcOverDue();
    }

    long calcOverDue() {
        // random the over due to avoid the concentrated closing
        return TimeUnit.MINUTES.toMillis(getOverdueInMinutes()) + ThreadLocalRandom.current()
            .nextLong(TimeUnit.MINUTES.toMillis(getOverdueInMinutes()) * 2);
    }

    protected long getOverdueInMinutes() {
        return MYSQL_DEFAULT_OVERDUE_IN_MINUTES;
    }

    protected SqlServerPacketDecoder newDecoder() {
        return new MySqlServerPacketDecoder();
    }

    protected void addAdditionalSessionCfg(DBConnectionInfo info) {
        if (this.dbConnInfo.getRole() != DBRole.SLAVE && !isAutoCommit) {
            additionSessionCfgQueue.add("AUTOCOMMIT = 0");
        } else {
            additionSessionCfgQueue.add("AUTOCOMMIT = 1");
        }
        additionSessionCfgQueue.add("SQL_AUTO_IS_NULL = 0");
        additionSessionCfgQueue.add("NAMES utf8mb4");
        // convert map
        info.getAdditionalSessionCfg().forEach((k, v) -> {
            additionSessionCfgQueue.add(String.format("%s = %s", k, v));
        });
    }

    public long getBirthday() {
        return birthday;
    }

    public DBConnectionInfo getOriginDbConnInfo() {
        return dbConnInfo;
    }

    public boolean isReady() {
        return isReady;
    }

    public Channel getServerChannel() {
        return this.serverChannel;
    }

    public SqlServerPacketDecoder getSqlServerPacketDecoder() {
        return this.sqlServerPackedDecoder;
    }

    private static final AtomicInteger sessIdGen = new AtomicInteger();
    public final int sessId = sessIdGen.incrementAndGet();

    @Override public String toString() {
        StringBuilder sb = new StringBuilder(256);
        print(sb);
        return sb.toString();
    }

    public void print(StringBuilder sb) {
        sb.append("{");
        sb.append("connId=").append(connectionId);
        sb.append(", dbId=").append(activeQulifiedDbId);
        sb.append(", role=").append(this.getOriginDbConnInfo().getRole());
        if (logger.isDebugEnabled()) {
            sb.append(", isAutoCommit=").append(isAutoCommit);
        }
        if (getServerChannel() != null) {
            sb.append(", southLocalAddr=").append(getServerChannel().localAddress());
        }
        if (sqlServerPackedDecoder.getSqlCtx() != null) {
            sb.append(", sqlCtx=").append(sqlServerPackedDecoder.getSqlCtx().hashCode());
        }
        sb.append("} ");
    }

    public long getConnectionId() {
        return connectionId;
    }

    public enum SERVER_SESSION_STATUS {
        INIT, HANDSHAKE_AUTH, AUTH_RESULT_SETAUTOCOMMIT, SET_ADDITIONAL_CFG, RECEIVE_LAST_OK, QUERY, QUERY_RESULT, QUIT
    }

    public void connect(ResponseHandler handler) throws QuitException {
        this.handler = handler;
        execute();
    }

    public boolean isOverdue() {
        return (System.currentTimeMillis() - getBirthday()) > this.overdue;
    }

    public void setServerPackets(Queue<byte[]> packets) {
        this.serverPackets = packets;
    }

    public synchronized void setStatus(SERVER_SESSION_STATUS status) {
        if (this.status != SERVER_SESSION_STATUS.QUIT) {
            this.status = status;
        }
    }

    private void sendQueryToDB(String query) {
        Com_Query cmdQuery = new Com_Query();
        cmdQuery.setQuery(query);
        previousAdditionalSessionCfg = query;
        dbServerWriteAndFlush(cmdQuery.toPacket(), (s) -> {
            logger.error("Cannot send query buf to server in sendQuery. " + this.dbConnInfo
                .getQualifiedDbId());
            doQuit();
        });
    }

    public void dbServerWriteAndFlush(byte[] bytesBuf, ServerSessionErrorHandler handler) {
        ByteBuf buf = Unpooled.wrappedBuffer(bytesBuf);
        serverChannel.writeAndFlush(buf).addListener(future -> {

            if (!future.isSuccess() && handler != null) {
                SqlSessionContext ctx = this.getSqlServerPacketDecoder().getSqlCtx();
                if (ctx != null) {
                    SchedulerWorker.getInstance().enqueue(
                        new CallBackManager(ctx, future, CallBackStatus.SERVER_FLUSH)
                            .setErrorHandler(handler).setServerSession(this));
                } else {
                    handler.invoke(this);
                }
            }
        });
    }

    private void doInit() throws QuitException {
        connectToDB();
    }

    private void connectToDB() throws QuitException {
        Bootstrap b = new Bootstrap();
        b.group(AthenaEventLoopGroupCenter.getSeverWorkerGroup());
        b.channel(AthenaEventLoopGroupCenter.getChannelClass());
        b.option(ChannelOption.SO_KEEPALIVE, true);
        if (AthenaEventLoopGroupCenter.getChannelClass().equals(EpollSocketChannel.class)) {
            b.option(EpollChannelOption.EPOLL_MODE, EpollMode.LEVEL_TRIGGERED);
        }
        b.handler(new ChannelInitializer<Channel>() {
            @Override protected void initChannel(Channel ch) throws Exception {
                ServerSession.this.sqlServerPackedDecoder.setServerSession(ServerSession.this);
                ch.pipeline().addLast(ServerSession.this.sqlServerPackedDecoder);
            }
        });

        if (this.status.equals(SERVER_SESSION_STATUS.INIT)) {
            setStatus(SERVER_SESSION_STATUS.HANDSHAKE_AUTH);
        }

        b.connect(dbConnInfo.getHost(), dbConnInfo.getPort())
            .addListener(new ChannelFutureListener() {

                @Override public void operationComplete(ChannelFuture future) throws Exception {
                    if (!future.isSuccess()) {
                        String errorMessage = String
                            .format("mysql db server [%s:%d]  connection failed",
                                ServerSession.this.dbConnInfo.getHost(),
                                ServerSession.this.dbConnInfo.getPort());
                        logger.error(errorMessage);
                        doQuit();
                    }
                }
            });
    }

    protected boolean doHandshakeAndAuth() throws QuitException {
        byte[] packet = serverPackets.poll();
        CmdHandshakeReal cmdHandshake = new CmdHandshakeReal(packet);
        cmdHandshake.execute();
        this.connectionId = cmdHandshake.handshake.connectionId;
        CmdAuthReal auth = new CmdAuthReal(this.dbConnInfo, cmdHandshake.handshake);
        auth.execute();
        setStatus(SERVER_SESSION_STATUS.AUTH_RESULT_SETAUTOCOMMIT);
        dbServerWriteAndFlush(auth.getSendBuf(), (s) -> {
            logger.error("Cannot send auth buf to server in doHandshakeAndAuth. " + this.dbConnInfo
                .getQualifiedDbId());
            doQuit();
        });
        return false;
    }

    protected void doAuthResultAndSetAutocommit() throws QuitException {
        byte[] packet = serverPackets.poll();
        CmdAuthResultReal authResult = new CmdAuthResultReal(packet);
        authResult.execute();
        String query = "SET " + additionSessionCfgQueue.poll();
        if (additionSessionCfgQueue.isEmpty()) {
            setStatus(SERVER_SESSION_STATUS.RECEIVE_LAST_OK);
        } else {
            setStatus(SERVER_SESSION_STATUS.SET_ADDITIONAL_CFG);
        }
        sendQueryToDB(query);
    }

    private void doSetAdditionalCfg() throws QuitException {
        byte[] packet = serverPackets.poll();
        if (Packet.getType(packet) != Flags.OK) {
            String msg = String
                .format("Not OK packet received after [%s],remain %s", previousAdditionalSessionCfg,
                    additionSessionCfgQueue);
            throw new QuitException(msg);
        }
        String query = "SET " + additionSessionCfgQueue.poll();
        if (additionSessionCfgQueue.isEmpty()) {
            setStatus(SERVER_SESSION_STATUS.RECEIVE_LAST_OK);
        } else {
            setStatus(SERVER_SESSION_STATUS.SET_ADDITIONAL_CFG);
        }
        sendQueryToDB(query);
    }

    protected void doRecieveLastOK() throws QuitException {
        byte[] packet = serverPackets.poll();
        if (Packet.getType(packet) != Flags.OK) {
            String msg = String
                .format("Not OK packet received after [%s],remain %s", previousAdditionalSessionCfg,
                    additionSessionCfgQueue);
            throw new QuitException(msg);
        }
        isReady = true;
        handleReady();
    }

    protected void handleReady() throws QuitException {
        if (this.handler == null) {
            return;
        }
        try {
            SqlSessionContext ctx = this.getSqlServerPacketDecoder().getSqlCtx();
            if (ctx == null) {
                this.handler.sessionAcquired(this);
            } else {
                SchedulerWorker.getInstance()
                    .enqueue(new ResponseHandlerManager(ctx, handler, this));
            }
        } catch (Exception e) {
            throw new QuitException(e.getMessage(), e);
        }
    }

    public synchronized void doQuit() {
        SqlSessionContext ctx = this.sqlServerPackedDecoder.getSqlCtx();
        releaseTrxSemaphore();
        if (ctx != null) {
            SchedulerWorker.getInstance().enqueue(new ServerSessionQuitManager(ctx, this));
        } else {
            closeServerChannelGracefully();
            logger.debug("serverSession {} is called by doQuit.", this);
        }
    }

    public void execute() {
        logger.debug("server status = " + status);
        boolean failed = false;
        try {
            switch (status) {
                case INIT:
                    doInit();
                    break;
                case HANDSHAKE_AUTH:
                    doHandshakeAndAuth();
                    break;
                case AUTH_RESULT_SETAUTOCOMMIT:
                    doAuthResultAndSetAutocommit();
                    break;
                case SET_ADDITIONAL_CFG:
                    doSetAdditionalCfg();
                    break;
                case RECEIVE_LAST_OK:
                    doRecieveLastOK();
                    break;
                case QUIT:
                    doQuit();
                    break;
                default:
                    logger.error("Unknown status! " + status);
            }
        } catch (AuthQuitException e) {
            logger.error(String
                .format("Failed to authenticate to mysql db : [%s],message : [%s]", dbConnInfo,
                    e.getMessage()), e);
            failed = true;
        } catch (Exception t) {
            failed = true;
            logger.error("ServerSession Error " + dbConnInfo.getQualifiedDbId(), t);
            // t.printStackTrace();
        } finally {
            if (failed) {
                setStatus(SERVER_SESSION_STATUS.QUIT);
                doQuit();
            }

        }

    }

    public void closeServerSession() {
        releaseTrxSemaphore();
        logger.warn(String.format("close server session : [%s]", this));
        pool.closeServerSession(this);
    }

    public void returnServerSession() throws QuitException {
        releaseTrxSemaphore();
        pool.returnServerSession(this);
    }

    public boolean isInTranscation() {
        return isInTranscation;
    }

    public void setInTranscation(boolean isInTranscation) {
        this.isInTranscation = isInTranscation;
    }

    public void setServerChannel(Channel serverChannel) {
        this.serverChannel = serverChannel;
    }

    public boolean isNeedCloseClient() {
        return isNeedCloseClient;
    }

    public void setNeedCloseClient(boolean isNeedCloseClient) {
        this.isNeedCloseClient = isNeedCloseClient;
    }

    private void setActiveQulifiedDbId(String activeQulifiedDbId) {
        this.activeQulifiedDbId = activeQulifiedDbId;
    }

    public void setActiveQulifiedInfo(DBConnectionInfo dbInfo) {
        setActiveQulifiedDbId(dbInfo.getQualifiedDbId());
        setActiveDbGroup(dbInfo.getGroup());
        setActiveDbId(dbInfo.getId());
    }

    public String getActiveQulifiedDbId() {
        return activeQulifiedDbId;
    }

    private void setActiveDbGroup(String activeDbGroup) {
        this.activeDbGroup = activeDbGroup;
    }

    private void setActiveDbId(String activeDbId) {
        this.activeDbId = activeDbId;
    }

    public String getActiveGroup() {
        return activeDbGroup;
    }

    public String getActiveDBId() {
        return activeDbId;
    }

    public DBRole getRole() {
        return dbConnInfo.getRole();
    }

    public String getHost() {
        return dbConnInfo.getHost();
    }

    public int getPort() {
        return dbConnInfo.getPort();
    }

    public String getDatabase() {
        return dbConnInfo.getDatabase();
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

    public void closeServerChannelGracefully() {
        Channel tmp = this.serverChannel;
        if (Objects.isNull(tmp)) {
            return;
        }
        if (!isClosed.getAndSet(true)) {
            tmp.writeAndFlush(Unpooled.wrappedBuffer(newQuitPackets()))
                .addListener(f -> tmp.close());
        } else {
            tmp.close();
        }
    }

    public static ServerSession newServerSession(ServerSessionPool pool) {
        DBConnectionInfo info = pool.getDbConnInfo();
        switch (info.getDBVendor()) {
            case MYSQL:
                return new ServerSession(info, pool);
            case PG:
                return new PGServerSession(info, pool);
            default:
                return new ServerSession(info, pool);
        }
    }

    protected byte[] newQuitPackets() {
        return Com_Quit.getQuitPacketBytes();
    }

    final AtomicReference<AutotreadHandler> autotreadHandlerRef =
        new AtomicReference<>(new AutotreadHandler());

    public AutotreadHandler closeAutoRead() {
        AutotreadHandler handler = new AutotreadHandler();
        AutotreadHandler pre = autotreadHandlerRef.getAndSet(handler);
        pre.isvalid.set(false);
        ServerSession.this.getServerChannel().config().setAutoRead(false);
        return handler;

    }

    public class AutotreadHandler {
        final private AtomicBoolean isvalid = new AtomicBoolean(true);

        public void openAutoRead() {
            if (isvalid.getAndSet(false)) {
                ServerSession.this.getServerChannel().config().setAutoRead(true);
            }
        }
    }
}
