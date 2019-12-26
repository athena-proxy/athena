package me.ele.jarch.athena.netty.debug;

import me.ele.jarch.athena.allinone.DBConnectionInfo;
import me.ele.jarch.athena.allinone.DBRole;
import me.ele.jarch.athena.allinone.HeartBeatCenter;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.netty.AthenaFrontServer;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.netty.state.SESSION_STATUS;
import me.ele.jarch.athena.scheduler.*;
import me.ele.jarch.athena.server.pool.ServerSessionPoolCenter;
import me.ele.jarch.athena.util.*;
import me.ele.jarch.athena.worker.SchedulerWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.BlockingQueue;

public class SystemInfo {

    private static final Logger logger = LoggerFactory.getLogger(SystemInfo.class);

    private static void addDebugInfo(StringBuilder sb, String s) {
        final String le = System.lineSeparator();
        sb.append(s).append(le);
    }

    public static String getInfo() {
        Map<String, Object> root = new LinkedHashMap<>();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        root.put("current time", sdf.format(new Date()));
        root.put("start time",
            sdf.format(new Date(ManagementFactory.getRuntimeMXBean().getStartTime())));
        root.put("pid", Constants.PID);
        root.put("APPID", Constants.APPID);
        root.put("HOSTNAME", Constants.HOSTNAME);
        root.put("DB conn", AthenaConfig.getInstance().getDatabaseConnectString());
        root.put("connectPath", AthenaConfig.getInstance().getConnectPath());
        root.put("zookeeperPath", AthenaConfig.getInstance().getZookeeperPath());

        List<Object> ctxInTran = new ArrayList<>();
        root.put("transaction context", ctxInTran);
        HangSessionMonitor.getAllCtx().stream().filter(SqlSessionContext::isInTransStatus)
            .forEach(ctx -> {
                ctxInTran.add(ctx.toString());
            });

        List<Object> ctxInQuery = new ArrayList<>();
        root.put("running query context", ctxInQuery);
        HangSessionMonitor.getAllCtx().stream().filter(
            ctx -> (ctx.getStatus() == SESSION_STATUS.QUERY_RESULT
                || ctx.getStatus() == SESSION_STATUS.FAKE_SQL)).forEach(ctx -> {
            ctxInQuery.add(ctx.toString());
        });

        List<Object> ctxInGrayQuery = new ArrayList<>();
        root.put("gray running query context", ctxInGrayQuery);
        HangSessionMonitor.getAllCtx().stream()
            .filter(ctx -> ctx.getStatus() == SESSION_STATUS.GRAY_RESULT).forEach(ctx -> {
            ctxInGrayQuery.add(ctx.toString());
        });

        root.put("DBChannelCfg", getDBChannelCfg());

        List<Map<String, Object>> pools = new ArrayList<>();
        root.put("POOL", pools);
        ServerSessionPoolCenter.getEntities().forEach((identifier, pool) -> {
            pools.addAll(convertString(
                pool.toString().replaceAll("[0-9]+[)]", "").replaceAll("[\\[]?[\\]]?", ""), "Pool",
                ",", ":"));
        });

        // parse HeartBeatCenter's info
        String HBCString = HeartBeatCenter.getInstance().toString();
        if (HBCString.indexOf("results") == -1) {
            root.put("HeartBeatCenter", "");
        } else {
            String HBCSubStr1 =
                HBCString.substring("HeartBeatCenter".length(), HBCString.indexOf("results"))
                    .replace("[", "").replace("]", "");
            Map<String, Object> HBCMap = convertString(HBCSubStr1, ",", "=");
            if (HBCString.indexOf("HeartBeatResult") == -1) {
                HBCMap.put("HeartBeatResult", "");
            } else {
                String HBCSubStr2 =
                    HBCString.substring(HBCString.indexOf("HeartBeatResult")).replace("[", "")
                        .replace("]", "");
                HBCMap
                    .put("HeartBeatResult", convertString(HBCSubStr2, "HeartBeatResult", ",", "="));
            }
            root.put("HeartBeatCenter", HBCMap);
        }

        List<String> queue = new ArrayList<>();
        root.put("scheduler worker queue", queue);
        BlockingQueue<Runnable>[] queues = SchedulerWorker.getInstance().getQueues();
        for (int i = 0; i < queues.length; i++) {
            queue.add("worker-index=" + i + " queue-size=" + queues[i].size());
        }

        root.put("MulScheduler", MulScheduler.getInstance().getInfo());
        Map<String, Integer> oneSecondQPS = new TreeMap<>();
        Map<String, Integer> tenSecondsQPS = new TreeMap<>();
        DBChannelDispatcher.getHolders()
            .forEach((groupName, holder) -> holder.getScheds().forEach(scheduler -> {
                oneSecondQPS.put(scheduler.getChannelName() + "." + scheduler.getName(),
                    QPSCounter.getInstance().getQPS(scheduler.getDbQueueId(), 1000));
                tenSecondsQPS.put(scheduler.getChannelName() + "." + scheduler.getName(),
                    QPSCounter.getInstance().getQPS(scheduler.getDbQueueId(), 10 * 1000));
            }));
        root.put("HasListenedPorts", AthenaFrontServer.getInstance().hasListenedPorts());
        Map<String, Object> qpsInteval = new LinkedHashMap<>();
        qpsInteval.put("1 second qps", oneSecondQPS);
        qpsInteval.put("10 seconds qps", tenSecondsQPS);
        root.put("QPSCounter", qpsInteval);
        root.put("RuntimeArgs", convertString(runtimeArgs, "\n", "="));
        return SafeJSONHelper.of(JacksonObjectMappers.getPrettyMapper())
            .writeValueAsStringOrDefault(root, "{}");
    }

    private static Map<String, Object> parseSingleDBChannelCommonCfg(
        DBChannelDispatcher dbChannelDispatcher) {
        final ZKCache zkCache = dbChannelDispatcher.getZKCache();
        Map<String, Object> DBChanCMapItme = new LinkedHashMap<>();

        // parse DBChannelCfg's info
        DBChanCMapItme.put("DBChannelCfg", dbChannelDispatcher.getCfg());
        DBChanCMapItme.put("reject sql pattern", zkCache.getRejectSQLByPattern());
        DBChanCMapItme.put("reject sql regular exp", zkCache.getRejectSQLByRegularExpPattern());
        DBChanCMapItme.put("auto killer open", zkCache.isAutoKillerOpen());
        DBChanCMapItme.put("auto killer lower size", zkCache.getLowerAutoKillerSize());
        DBChanCMapItme.put("auto killer upper size", zkCache.getUpperAutoKillerSize());
        DBChanCMapItme.put("auto killer slow sql", zkCache.getAutoKillerSlowSQL());
        DBChanCMapItme.put("auto killer slow commit", zkCache.getAutoKillerSlowCommit());
        DBChanCMapItme.put("max queue size", zkCache.getMaxQueueSize());
        DBChanCMapItme.put("client conn count", dbChannelDispatcher.cCnt.get());

        return DBChanCMapItme;
    }

    private static boolean isStandbyMaster(DBConnectionInfo info) {
        return (info.getRole() == DBRole.MASTER) && info.isReadOnly();
    }

    private static void parseDBScheduler(Scheduler scheduler, Map<String, Object> dbConnInfoMap) {
        dbConnInfoMap.put("trigger auto kill", AutoKiller.isSchedulerInAutoKill(scheduler));
        dbConnInfoMap.put("Semaphore Permits", scheduler.getSemaphoreAvailablePermits());
        dbConnInfoMap.put("MaxActiveDBSessions", scheduler.getMaxActiveDBSessions());
    }

    private static List<Map<String, Object>> getDBChannelCfg() {
        List<Map<String, Object>> retList = new ArrayList<>();

        new TreeMap<>(DBChannelDispatcher.getHolders()).forEach((groupName, holder) -> {
            Map<String, Object> dbChannelCommonCfg = parseSingleDBChannelCommonCfg(holder);

            List<Object> dbInfoList = new ArrayList<>();
            holder.getDbGroups().forEach((dbGroupName, dbGroup) -> {
                holder.getScheds().stream()
                    .filter(scheduler -> scheduler.getInfo().getGroup().equals(dbGroupName))
                    .forEach(scheduler -> {
                        Map<String, Object> dbConnInfoMap = new LinkedHashMap<>();
                        String dbInfoStr = scheduler.getInfo().toString();
                        if (!isStandbyMaster(scheduler.getInfo())) {
                            parseDBScheduler(scheduler, dbConnInfoMap);
                        }
                        if (dbInfoStr.contains("[")) {
                            String DBConnSubStr = dbInfoStr
                                .substring(dbInfoStr.indexOf("[") + 1, dbInfoStr.length() - 1);
                            dbConnInfoMap
                                .put("DBConnectionInfo", convertString(DBConnSubStr, ",", "="));
                        } else {
                            dbConnInfoMap.put("DBConnectionInfo", "");
                        }

                        dbInfoList.add(dbConnInfoMap);
                    });
            });

            dbChannelCommonCfg.put("DBConnectionInfo", dbInfoList);
            retList.add(dbChannelCommonCfg);
        });

        return retList;
    }

    /**
     * convert String to List
     *
     * @param s
     * @param separatorForArray the separator for array
     * @param separatorForKV    the separator between tow key-values
     * @param connector         the connector in one key-value
     * @return
     */
    public static List<Map<String, Object>> convertString(String s, String separatorForArray,
        String separatorForKV, String connector) {
        if (s == null || "".equals(s.trim()))
            return null;

        if (separatorForArray == null || "".equals(separatorForArray.trim()))
            return null;

        if (!s.contains(separatorForArray))
            return null;

        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        String[] array = s.split(separatorForArray);
        for (String a : array) {
            if (a == null || "".equals(a.trim()))
                continue;

            list.add(convertString(a, separatorForKV, connector));
        }

        return list;
    }

    /**
     * convert String to simple Map
     *
     * @param s
     * @param separator the separator between tow key-values
     * @param connector the connector in one key-value
     * @return
     */
    public static Map<String, Object> convertString(String s, String separator, String connector) {
        if (s == null || "".equals(s.trim()))
            return null;

        if (separator == null) // "\n".trim() is ""
            return null;

        if (connector == null || "".equals(connector.trim()))
            return null;

        if (!s.contains(separator) || !s.contains(connector))
            return null;

        Map<String, Object> map = new HashMap<>();
        String[] KVs = s.split(separator);
        for (String KVItem : KVs) {
            int index = KVItem.indexOf(connector);
            if (index == -1)
                continue;

            map.put(KVItem.substring(0, index).trim(), KVItem.substring(index + 1).trim());
        }
        return map;
    }

    private static final String runtimeArgs;

    static {
        runtimeArgs = getRuntimeArgs();
    }

    private static String getRuntimeArgs() {
        StringBuilder sb = new StringBuilder();
        new TreeSet<String>(ManagementFactory.getRuntimeMXBean().getInputArguments())
            .forEach(x -> addDebugInfo(sb, x));
        return sb.toString();
    }
}
