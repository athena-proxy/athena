package me.ele.jarch.athena.allinone;

import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.server.async.MasterHeartBeat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class HeartBeatCenter {
    private static final Logger logger = LoggerFactory.getLogger(HeartBeatCenter.class);
    private static int maxMissedHeartbeat = 2;
    private Map<DBConnectionId, HeartBeatHolder> dbHbGroupMap = new ConcurrentHashMap<>();

    private static final HeartBeatCenter INSTANCE = new HeartBeatCenter();

    public static HeartBeatCenter getInstance() {
        return INSTANCE;
    }

    // private AllInOne allInOne;
    // 记录HeartBeatCenter启动时间，用于过滤heartbeat metrics在启动时的一些误报
    public static final long startUpTime = System.currentTimeMillis();

    static public void config(String uri) {
        try {
            logger.info("config HeartBeatCenter " + uri);
            String parameterList = uri.trim().split("/heartbeat\\?")[1];
            String[] parameters = parameterList.trim().split("&");
            for (String parameter : parameters) {
                String[] items = parameter.split("=");
                String key = items[0].trim();
                String value = items[1].trim();
                if (key.equals("MAX_MISSING_HEARTBEAT")) {
                    int maxMissingHeartbeat = Integer.valueOf(value);
                    if (maxMissingHeartbeat >= 1) {
                        maxMissedHeartbeat = maxMissingHeartbeat;
                    }
                }

            }
        } catch (Exception e) {
            logger.warn("Failed to config HeartBeatCenter " + uri, e);
        }
    }

    public MasterHeartBeat createHeartBeat(DBConnectionInfo info, DBGroup dbGroup) {
        DBConnectionId dbId = DBConnectionId.buildId(info);
        HeartBeatHolder hbHolder =
            dbHbGroupMap.computeIfAbsent(dbId, k -> new HeartBeatHolder(info));
        return hbHolder.startHeartBeat(dbGroup);
    }

    public void deleteHeartBeat(DBConnectionInfo info, DBGroup dbGroup) {
        DBConnectionId dbId = DBConnectionId.buildId(info);
        HeartBeatHolder hbHolder = dbHbGroupMap.getOrDefault(dbId, null);
        if (Objects.isNull(hbHolder)) {
            logger.error("Trying to delete a none exist heartbeat, DBConnectionInfo {}", info);
            return;
        }
        hbHolder.stopHeartBeat(dbGroup);
        if (hbHolder.getDbGroups().isEmpty()) {
            hbHolder.getHeartBeat().destroy();
            dbHbGroupMap.remove(dbId);
        }
    }

    public HeartBeatResult getDBStatus(DBConnectionInfo dbInfo) {
        DBConnectionId dbId = DBConnectionId.buildId(dbInfo);
        HeartBeatHolder hbHolder = dbHbGroupMap.getOrDefault(dbId, null);
        if (Objects.isNull(hbHolder)) {
            logger.error("Trying to get dbstatus for a none exist heartbeat, DBConnectionInfo {}",
                dbInfo);
            return null;
        }
        return hbHolder.getHeartBeat().getResult();
    }

    @Override public String toString() {
        StringBuilder sb = new StringBuilder();
        dbHbGroupMap.forEach((dbId, heartBeatHolder) -> {
            sb.append(dbId).append(heartBeatHolder.getHeartBeat().getResult());
            sb.append(System.lineSeparator());
        });
        return "HeartBeatCenter [MAX_MISSING_HEARTBEAT=" + maxMissedHeartbeat + ",\nresults=\n" + sb
            .toString() + "]";
    }

    public static int getMaxMissedHeartbeat() {
        return maxMissedHeartbeat;
    }

    public static boolean hasStartUp() {
        return System.currentTimeMillis() > startUpTime + Constants.STARTUP_DELAY_TIME;
    }
}
