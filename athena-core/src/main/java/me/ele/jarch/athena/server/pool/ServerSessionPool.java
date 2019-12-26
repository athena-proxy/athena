package me.ele.jarch.athena.server.pool;

import io.netty.channel.ChannelConfig;
import io.netty.util.AbstractReferenceCounted;
import me.ele.jarch.athena.allinone.AllInOneMonitor;
import me.ele.jarch.athena.allinone.DBConnectionInfo;
import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.netty.SqlSessionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ServerSessionPool extends AbstractReferenceCounted {
    private static final Logger logger = LoggerFactory.getLogger(ServerSessionPool.class);
    private static int initServerSessionNum = 1;

    private final DBConnectionInfo dbConnInfo;
    private final PoolId poolId;
    private final ServerSessionFactory factory;
    private volatile boolean active = true;
    // 对于非SLAVE建立isAutoCommit所标识的标志位
    // 对于SLAVE强制建立autocommit的ServerSession
    private final boolean isAutoCommit;

    public static void setInitServerSessionNum(int num) {
        logger.warn("initServerSessionNum changed from " + initServerSessionNum + " to " + num);
        ServerSessionPool.initServerSessionNum = num;
    }

    public static int getInitServerSessionNum() {
        return initServerSessionNum;
    }

    public DBConnectionInfo getDbConnInfo() {
        return dbConnInfo;
    }

    private ConcurrentLinkedQueue<ServerSession> idleConns = new ConcurrentLinkedQueue<>();

    // the session has been got by SqlSessionContext
    private final AtomicInteger activeConnCount = new AtomicInteger(0);
    // all created session count time since the pool was born.
    private final AtomicLong connTimeCount = new AtomicLong(0);
    // all created session count since the pool was born.
    private final AtomicInteger totalConnCount = new AtomicInteger(0);
    // all closed session count since the pool was born.
    private final AtomicInteger closedConnCount = new AtomicInteger(0);

    @Override public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Pool [" + dbConnInfo.getQualifiedDbId() + "],");
        sb.append("closedConnectionCount: [" + closedConnCount.get() + "],");
        sb.append("connectionTimeCount: [" + connTimeCount.get() + "],");
        sb.append("totalConnectionCount: [" + totalConnCount.get() + "],");
        sb.append("idleConnectionIds: [");
        idleConns.forEach(s -> sb.append(s.sessId).append(" "));
        sb.append("],");
        sb.append("activeConnectionCount: [" + activeConnCount.get() + "]");
        return sb.toString();
    }

    ServerSessionPool(DBConnectionInfo info, boolean isAutoCommit, ServerSessionFactory factory) {
        this.dbConnInfo = info;
        this.poolId = new PoolId(dbConnInfo, isAutoCommit);
        this.isAutoCommit = isAutoCommit;
        this.factory = factory;
        for (int i = 0; i < initServerSessionNum; i++) {
            initServerSession();
        }
    }

    private void initServerSession() {
        try {
            factory.make(this, null, new ResponseHandler() {

                @Override public void sessionAcquired(ServerSession session) {
                    if (!active) {
                        return;
                    }
                    idleConns.add(session);
                    doConnStatistics(session, true);
                }
            });
        } catch (QuitException e) {
            logger.error("Failed to create connection during ServerSessionPool Initialization : "
                + dbConnInfo, e);
        }
    }

    public void acquireServerSession(final DBConnectionInfo dbInfo, final ResponseHandler handler,
        final SqlSessionContext sqlCtx) throws QuitException {
        if (!active) {
            throw new QuitException("get server session from inactive DB server");
        }

        while (!idleConns.isEmpty()) {
            ServerSession session = idleConns.poll();
            if (session == null)
                break;

            if (!session.isOverdue() && session.getServerChannel().isActive()) {
                activeConnCount.incrementAndGet();
                session.getSqlServerPacketDecoder().setSqlCtx(sqlCtx);
                session.setActiveQulifiedInfo(dbInfo);
                handler.sessionAcquired(session);
                return;
            } else {
                logger.warn(String.format("Timed out,close server session : [%s]", session));

                // cannot call closeServerSession,because serverSession hasnot been got by SqlSessionContext
                closedConnCount.incrementAndGet();
                session.closeServerChannelGracefully();
                continue;
            }

        }

        // create session
        factory.make(this, sqlCtx, new ResponseHandler() {
            @Override public void sessionError(Throwable t, ServerSession session) {
                handler.sessionError(t, session);
            }

            @Override public void sessionAcquired(ServerSession session) {
                session.setActiveQulifiedInfo(dbInfo);
                doConnStatistics(session, false);
                handler.sessionAcquired(session);
            }
        });
    }

    private void doConnStatistics(ServerSession serverSession, boolean isInit) {
        long sessionTime = System.currentTimeMillis() - serverSession.getBirthday();
        int currTotalConnCount = totalConnCount.incrementAndGet();
        if (!isInit) {
            activeConnCount.incrementAndGet();
        }
        int currActiveConnCount = activeConnCount.get();
        long currConnTimeCount = connTimeCount.addAndGet(sessionTime);
        int idleConnCount = totalConnCount.get() - activeConnCount.get() - closedConnCount.get();
        logger.info(String.format(
            "New connection created: [%s],time: [%s],totalConnCount: [%s],connTimeCount: [%s],activeConnCount: [%s],idleConnCount: [%s]",
            serverSession, sessionTime, currTotalConnCount, currConnTimeCount, currActiveConnCount,
            idleConnCount));
    }

    public void returnServerSession(ServerSession serverSession) {
        activeConnCount.decrementAndGet();
        serverSession.getSqlServerPacketDecoder().setSqlCtx(null);
        ChannelConfig channelConfig = serverSession.getServerChannel().config();
        if (!channelConfig.isAutoRead()) {
            logger.warn(String
                .format("big-result-autoread occur not close autoread serversession [ %s ]",
                    serverSession.toString()));
            channelConfig.setAutoRead(true);
        }

        idleConns.add(serverSession);
    }

    public void closeServerSession(ServerSession serverSession) {
        activeConnCount.decrementAndGet();
        closedConnCount.incrementAndGet();
        serverSession.closeServerChannelGracefully();
    }

    public synchronized void closePoolConnections() {
        this.active = false;
        if (activeConnCount.get() > 0) {
            AllInOneMonitor.getInstance().attachInavtiveServer(this);
        }
        this.clean();
    }

    public synchronized void tryWarmUp(int expectActiveCount) {
        if (!active) {
            return;
        }
        if (activeConnCount.get() >= expectActiveCount) {
            return;
        }

        tryDrainIdlePool(expectActiveCount);

        int warmUpCount = expectActiveCount - activeConnCount.get() - idleConns.size();

        while (warmUpCount > 0) {
            initServerSession();
            warmUpCount--;
        }
    }

    /**
     * 检查当前idel pool中的连接是否可用。
     * 基于ConcurrentLinkedQueue的FIFO特性，拿出一个idle连接，若可用则放回池中
     */
    private void tryDrainIdlePool(int expectActiveCount) {
        int idleCount = idleConns.size();
        int count = expectActiveCount > idleCount ? idleCount : expectActiveCount;
        for (int i = 0; i < count; i++) {
            ServerSession session = idleConns.poll();
            if (Objects.isNull(session)) {
                break;
            }
            if (!session.isOverdue() && session.getServerChannel().isActive()) {
                idleConns.add(session);
            } else {
                closedConnCount.incrementAndGet();
                session.closeServerChannelGracefully();
            }
        }
    }

    private void clean() {
        ServerSession session = this.idleConns.poll();
        while (session != null) {
            logger.info(String.format("close server session : [%s] , closed by : [%s]", session,
                this.dbConnInfo));
            closedConnCount.incrementAndGet();
            session.closeServerChannelGracefully();
            session = this.idleConns.poll();
        }
    }

    public boolean isAutoCommit() {
        return isAutoCommit;
    }

    @Override protected void deallocate() {
        logger.debug(
            "close server session pool : original qualifiedDbId={}, host={}, port={}, database={}, user={}, role={}",
            dbConnInfo.getQualifiedDbId(), dbConnInfo.getHost(), dbConnInfo.getPort(),
            dbConnInfo.getDatabase(), dbConnInfo.getUser(), dbConnInfo.getRole());
        closePoolConnections();
        ServerSessionPoolCenter.getEntities().remove(poolId);
    }
}
