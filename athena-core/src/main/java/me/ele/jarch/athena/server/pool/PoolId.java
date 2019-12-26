package me.ele.jarch.athena.server.pool;

import me.ele.jarch.athena.allinone.DBConnectionInfo;
import me.ele.jarch.athena.allinone.DBRole;

import java.util.Map;
import java.util.TreeMap;

/**
 * Created by jinghao.wang on 16/9/12.
 */
public class PoolId {
    private String database = "";
    private String host = "";
    private int port;
    private String user = "";
    private DBRole role = DBRole.DUMMY;
    private boolean isAutoCommit;
    private Map<String, String> additionalSessionCfg = new TreeMap<>();

    public PoolId(DBConnectionInfo dbConnectionInfo, boolean isAutoCommit) {
        database = dbConnectionInfo.getDatabase();
        host = dbConnectionInfo.getHost();
        port = dbConnectionInfo.getPort();
        user = dbConnectionInfo.getUser();
        role = dbConnectionInfo.getRole();
        this.isAutoCommit = isAutoCommit;
        this.additionalSessionCfg = dbConnectionInfo.getAdditionalSessionCfg();
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        PoolId that = (PoolId) o;

        if (port != that.port)
            return false;
        if (isAutoCommit != that.isAutoCommit)
            return false;
        if (!database.equals(that.database))
            return false;
        if (!host.equals(that.host))
            return false;
        if (!user.equals(that.user))
            return false;
        if (role != that.role)
            return false;
        return additionalSessionCfg.equals(that.additionalSessionCfg);

    }

    @Override public int hashCode() {
        int result = database.hashCode();
        result = 31 * result + host.hashCode();
        result = 31 * result + port;
        result = 31 * result + user.hashCode();
        result = 31 * result + role.hashCode();
        result = 31 * result + (isAutoCommit ? 1 : 0);
        result = 31 * result + additionalSessionCfg.hashCode();
        return result;
    }

    @Override public String toString() {
        return "PoolId{" + "database='" + database + '\'' + ", host='" + host + '\'' + ", port="
            + port + ", user='" + user + '\'' + ", role=" + role + ", isAutoCommit=" + isAutoCommit
            + ", additionalSessionCfg=" + additionalSessionCfg + '}';
    }
}
