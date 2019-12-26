package me.ele.jarch.athena.util;

import me.ele.jarch.athena.scheduler.DBChannelDispatcher;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.stream.Collectors.toSet;

/**
 * Created by jinghao.wang on 16/9/8.
 */
public class WeakMasterFilter {
    private static Map<String, Set<String>> dalGroupWeakMasterMap = new ConcurrentHashMap<>();
    private static volatile Set<String> weakMasterDBGroups = Collections.emptySet();

    public synchronized static void update(DBChannelDispatcher dispatcher) {
        Set<String> newWeakMasterDBGroups = new TreeSet<>();
        if (dispatcher.getZKCache().isInWeakMasterMode()) {
            newWeakMasterDBGroups.addAll(dispatcher.getDbGroups().keySet());
        }
        dalGroupWeakMasterMap.put(dispatcher.getCfg(), newWeakMasterDBGroups);
        weakMasterDBGroups =
            dalGroupWeakMasterMap.values().stream().flatMap(Collection::stream).collect(toSet());
    }

    public static boolean isWeakMasterDBGroup(String dbGroupName) {
        return weakMasterDBGroups.contains(dbGroupName);
    }
}
