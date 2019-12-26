package me.ele.jarch.athena.util.etrace;

import me.ele.jarch.athena.allinone.DBConnectionInfo;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.scheduler.EmptyScheduler;
import me.ele.jarch.athena.scheduler.Scheduler;
import me.ele.jarch.athena.server.pool.ServerSession;

import java.util.Objects;

/**
 * Created by jinghao.wang on 2017/9/18.
 */
public class MetricMetaExtractor {

    private MetricMetaExtractor() {
    }

    /**
     * accept sqlCtx == null && sqlCtx.getHolder() == null
     *
     * @param sqlCtx
     * @return target main group name or "unknown"
     */
    public static String extractDALGroupName(SqlSessionContext sqlCtx) {
        if (Objects.isNull(sqlCtx)) {
            return "unknown";
        }
        if (Objects.isNull(sqlCtx.getHolder())) {
            return sqlCtx.sqlSessionContextUtil.getPotentialDalGroupName();
        }
        return sqlCtx.getHolder().getCfg();
    }

    public static String extractDBid(SqlSessionContext sqlCtx) {
        if (sqlCtx.scheduler != EmptyScheduler.emptySched && sqlCtx.scheduler.getInfo() != null) {
            return sqlCtx.scheduler.getInfo().getQualifiedDbId();
        }
        return "emptydb";
    }

    public static String extractDALGroupName(Scheduler scheduler) {
        String groupName = "unknown";
        if (Objects.nonNull(scheduler) && scheduler != EmptyScheduler.emptySched) {
            groupName = scheduler.getChannelName();
        }
        return groupName;
    }

    public static String extractDBid(Scheduler scheduler) {
        String dbId = "emptydb";
        if (Objects.nonNull(scheduler) && scheduler != EmptyScheduler.emptySched && Objects
            .nonNull(scheduler.getInfo()) && Objects
            .nonNull(scheduler.getInfo().getQualifiedDbId())) {
            dbId = scheduler.getInfo().getQualifiedDbId();
        }
        return dbId;
    }

    public static String extractDatabase(Scheduler scheduler) {
        String database = "emptydatabase";
        if (Objects.nonNull(scheduler) && scheduler != EmptyScheduler.emptySched && Objects
            .nonNull(scheduler.getInfo())) {
            database = scheduler.getInfo().getDatabase();
        }
        return database;
    }

    public static String extractDBIp(Scheduler scheduler) {
        String ip = "emptyip";
        if (Objects.nonNull(scheduler) && scheduler != EmptyScheduler.emptySched && Objects
            .nonNull(scheduler.getInfo())) {
            ip = scheduler.getInfo().getHost();
        }
        return ip;
    }

    public static String extractDBPort(Scheduler scheduler) {
        String port = "emptyport";
        if (Objects.nonNull(scheduler) && scheduler != EmptyScheduler.emptySched && Objects
            .nonNull(scheduler.getInfo())) {
            port = String.valueOf(scheduler.getInfo().getPort());
        }
        return port;
    }

    public static String extractDBRole(Scheduler scheduler) {
        String role = "emptyrole";
        if (Objects.nonNull(scheduler) && scheduler != EmptyScheduler.emptySched && Objects
            .nonNull(scheduler.getInfo())) {
            role = scheduler.getInfo().getRole().name();
        }
        return role;
    }

    public static String extractDBGroupName(DBConnectionInfo info) {
        String groupName = "unknown";
        if (Objects.nonNull(info) && Objects.nonNull(info.getGroup())) {
            groupName = info.getGroup();
        }
        return groupName;
    }

    public static String extractDBid(DBConnectionInfo info) {
        String dbId = "emptydb";
        if (Objects.nonNull(info) && Objects.nonNull(info.getQualifiedDbId())) {
            dbId = info.getQualifiedDbId();
        }
        return dbId;
    }

    public static String extractDatabase(DBConnectionInfo info) {
        String database = "emptydatabase";
        if (Objects.nonNull(info)) {
            database = info.getDatabase();
        }
        return database;
    }

    public static String extractDBIp(DBConnectionInfo info) {
        String ip = "emptyip";
        if (Objects.nonNull(info)) {
            ip = info.getHost();
        }
        return ip;
    }

    public static String extractDBPort(DBConnectionInfo info) {
        String port = "emptyport";
        if (Objects.nonNull(info)) {
            port = String.valueOf(info.getPort());
        }
        return port;
    }

    public static String extractDALGroupName(ServerSession serverSession) {
        String groupName = "unknown";
        if (Objects.nonNull(serverSession) && Objects.nonNull(serverSession.getActiveGroup())) {
            groupName = serverSession.getActiveGroup();
        }
        return groupName;
    }

    public static String extractDBid(ServerSession serverSession) {
        String dbId = "emptydb";
        if (Objects.nonNull(serverSession) && Objects
            .nonNull(serverSession.getActiveQulifiedDbId())) {
            dbId = serverSession.getActiveQulifiedDbId();
        }
        return dbId;
    }
}
