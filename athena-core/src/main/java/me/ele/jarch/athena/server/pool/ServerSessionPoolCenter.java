package me.ele.jarch.athena.server.pool;

import me.ele.jarch.athena.allinone.DBConnectionInfo;
import me.ele.jarch.athena.allinone.DBRole;
import me.ele.jarch.athena.allinone.DBVendor;
import me.ele.jarch.athena.netty.SqlSessionContext;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 获取和移除ServerSessionPool, 不要再直接new ServerSessionPool Created by jinghao.wang on 16/2/22.
 */
public class ServerSessionPoolCenter {
    private static final ServerSessionPoolCenter INSTANCE = new ServerSessionPoolCenter();
    private static final ServerSessionFactory serverSessionFactory =
        (ServerSessionPool pool, SqlSessionContext sqlCtx, ResponseHandler handler) -> {
            ServerSession session = ServerSession.newServerSession(pool);
            session.getSqlServerPacketDecoder().setSqlCtx(sqlCtx);
            session.connect(handler);
            return session;
        };
    private static final ServerSessionFactory sickServerSessionFactory =
        (ServerSessionPool sessionPool, SqlSessionContext sqlCtx, ResponseHandler handler) -> {
            SickServerSession session = SickServerSession.newSickServerSession(sessionPool);
            session.getSqlServerPacketDecoder().setSqlCtx(sqlCtx);
            session.connect(handler);
            return session;
        };

    // sick use
    private final DBConnectionInfo sickDBInfo = DBConnectionInfo.parseDBConnectionString(
        "fakegroup fakedb 0.0.0.0 3306 dummy sick fake ZHVtbXk=");
    private final DBConnectionInfo pgSickDBInfo = DBConnectionInfo.parseDBConnectionString(
        "fakegroup fakepgdb 0.0.0.0 5432 dummy pgsick fake ZHVtbXk= db_vendor=pg");
    // batchOp use
    private final DBConnectionInfo batchOperateDBInfo = DBConnectionInfo.parseDBConnectionString(
        "batchoperategroup batchoperatedb 0.0.0.0 3306 dummy dummy batch ZHVtbXk=");

    private final Map<PoolId, ServerSessionPool> entities = new ConcurrentHashMap<>();

    public static ServerSessionPoolCenter getInst() {
        return INSTANCE;
    }

    public static Map<PoolId, ServerSessionPool> getEntities() {
        return INSTANCE.entities;
    }

    /**
     * 根据DBConnectionInfo, 是否AutoCommit获取一个ServerSessionPoolManager, 如果没有则新建ServerSessionPool
     *
     * @param info
     * @param isAutoCommit
     * @return
     */
    public synchronized ServerSessionPool getServerSessionPool(DBConnectionInfo info,
        boolean isAutoCommit) {
        isAutoCommit = forceAutoCommit(info) ? true : isAutoCommit;
        PoolId id = new PoolId(info, isAutoCommit);
        ServerSessionPool pool = entities.get(id);
        if (Objects.isNull(pool)) {
            pool = new ServerSessionPool(info, isAutoCommit, serverSessionFactory);
            entities.put(id, pool);
        } else {
            pool.retain();
        }
        return pool;
    }

    public synchronized ServerSessionPool getSickServerSessionPool(DBVendor dbVendor) {
        DBConnectionInfo dbInfo = getSickDBInfo(dbVendor);
        PoolId id = new PoolId(dbInfo, true);
        ServerSessionPool pool = entities.get(id);
        if (Objects.isNull(pool)) {
            pool = new ServerSessionPool(dbInfo, true, sickServerSessionFactory);
            entities.put(id, pool);
        } else {
            pool.retain();
        }
        return pool;
    }

    private DBConnectionInfo getSickDBInfo(DBVendor dbVendor) {
        switch (dbVendor) {
            case MYSQL:
                return sickDBInfo;
            case PG:
                return pgSickDBInfo;
            default:
                return sickDBInfo;
        }
    }

    /**
     * 是否强制autocommit
     *
     * @param info
     * @return
     */
    private boolean forceAutoCommit(DBConnectionInfo info) {
        if (info.getRole() == DBRole.SLAVE) {
            return true;
        }
        if (info.getDBVendor() == DBVendor.PG) {
            return true;
        }
        return false;
    }

    public synchronized ServerSessionPool getBatchOperateServerSessionPool() {
        PoolId id = new PoolId(batchOperateDBInfo, true);
        ServerSessionPool pool = entities.get(id);

        if (Objects.isNull(pool)) {
            pool = new ServerSessionPool(batchOperateDBInfo, true,
                (ServerSessionPool sessionPool, SqlSessionContext sqlCtx, ResponseHandler handler) -> {
                    Objects.requireNonNull(sessionPool, "ServerSessionPool must be not null");
                    Objects.requireNonNull(handler, "ResponseHandler must be not null");
                    DBConnectionInfo info = sessionPool.getDbConnInfo();
                    ServerSession session = new BatchOperateServerSession(info, sessionPool);
                    session.getSqlServerPacketDecoder().setSqlCtx(sqlCtx);
                    session.connect(handler);
                    return session;
                });
            entities.put(id, pool);
        } else {
            pool.retain();
        }
        return pool;
    }
}
