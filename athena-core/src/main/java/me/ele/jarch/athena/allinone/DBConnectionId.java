package me.ele.jarch.athena.allinone;


import java.util.Objects;
import java.util.StringJoiner;

public class DBConnectionId {

    public final String id;

    private DBConnectionId(DBConnectionInfo info) {
        Objects.requireNonNull(info,
            () -> "DBConnHbId:DBConnectionInfo must not be null,info=" + info);
        id = buildConnectionString(info);
    }

    public static DBConnectionId buildId(DBConnectionInfo info) {
        return new DBConnectionId(info);
    }

    /**
     * build ID based on DBConnectionInfo's attribute: database, host, port, user, password to make sure
     * all the DBConnectionInfos for same physical database has the same ID
     */
    private static String buildConnectionString(DBConnectionInfo info) {
        StringJoiner sj = new StringJoiner("_");
        sj.add(info.getDatabase());
        sj.add(info.getHost());
        sj.add(String.valueOf(info.getPort()));
        sj.add(info.getUser());
        sj.add(String.valueOf(info.getPword().hashCode()));
        return sj.toString();
    }

    @Override public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        return result;
    }

    @Override public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        DBConnectionId other = (DBConnectionId) obj;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        return true;
    }

    @Override public String toString() {
        return "DBConnHbId [id=" + id + "]";
    }
}
