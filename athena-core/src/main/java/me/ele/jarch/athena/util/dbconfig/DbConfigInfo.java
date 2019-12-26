package me.ele.jarch.athena.util.dbconfig;

import me.ele.jarch.athena.allinone.DBRole;
import me.ele.jarch.athena.scheduler.DBChannelDispatcher;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class DbConfigInfo {
    public final String dbGroup;
    public final String database;
    public final String host;
    public final int port;
    public final String role;
    public final String id;

    public DbConfigInfo(String dbGroup, String database, String host, int port, String role,
        String id) {
        this.dbGroup = dbGroup;
        this.database = database;
        this.host = host;
        this.port = port;
        this.role = role;
        this.id = id;
    }

    public static List<DbConfigInfo> getDbConfigInfos(String dalGroup) {
        if (StringUtils.isEmpty(dalGroup)) {
            return Collections.emptyList();
        }
        DBChannelDispatcher dispatcher = DBChannelDispatcher.getHolders().get(dalGroup);
        if (Objects.isNull(dispatcher)) {
            return Collections.emptyList();
        }
        List<DbConfigInfo> result = new ArrayList<>();
        dispatcher.getDbGroups().values().stream()
            .flatMap(dbGroup -> dbGroup.getDbGroupUpdater().getAllInfos().values().stream())
            .forEach((dbInfo) -> {
                String dbGroup = dbInfo.getGroup();
                dbGroup = Objects.isNull(dbGroup) ? "" : dbGroup;
                String database = dbInfo.getDatabase();
                database = Objects.isNull(database) ? "" : database;
                String host = dbInfo.getHost();
                host = Objects.isNull(host) ? "" : host;
                DBRole dbRole = dbInfo.getRole();
                String role = Objects.isNull(dbRole) ? "" : dbRole.name().toLowerCase();
                String id = dbInfo.getId();
                id = Objects.isNull(id) ? "" : id;

                result.add(new DbConfigInfo(dbGroup, database, host, dbInfo.getPort(), role, id));
            });
        return result;
    }

    @Override public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("DbConfigInfo {dbGroup=");
        builder.append(dbGroup);
        builder.append(", database=");
        builder.append(database);
        builder.append(", host=");
        builder.append(host);
        builder.append(", port=");
        builder.append(port);
        builder.append(", role=");
        builder.append(role);
        builder.append(", id=");
        builder.append(id);
        builder.append("}");
        return builder.toString();
    }

}
