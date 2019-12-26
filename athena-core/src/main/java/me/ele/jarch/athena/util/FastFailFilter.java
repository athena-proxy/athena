package me.ele.jarch.athena.util;

import me.ele.jarch.athena.scheduler.DBChannelDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by warrior on 16/9/28.
 */
public class FastFailFilter {
    private static Logger logger = LoggerFactory.getLogger(FastFailFilter.class);
    private static volatile Map<String, Long> failFastDBGroups = new ConcurrentHashMap<>();

    /**
     * 每次ZKCache变动时，更新所有DBGroup对应的Fast Fail时间
     * 当多个ZKCache设置fast fail时, 使用生效时间最长的配置
     */
    public static void update() {
        Map<String, Long> newFailFastDBGroups = new ConcurrentHashMap<>();
        // 遍历所有ZKCache，用其中Fast Fail仍生效的组成新Map
        DBChannelDispatcher.getHolders().values().forEach(dispatcher -> {
            if (dispatcher.getZKCache().isInHeartbeatFastfailPeriod()) {
                dispatcher.getDbGroups().keySet().forEach(dbGroupName -> {
                    long newPeriod = dispatcher.getZKCache().getHeartbeatFastfailPeriodEnd();
                    // 如果新Map里之前已经插入了该DBGroup对应的值，且比现值小，则更新
                    if (newFailFastDBGroups.containsKey(dbGroupName)) {
                        if (newFailFastDBGroups.get(dbGroupName) < newPeriod) {
                            newFailFastDBGroups.put(dbGroupName, newPeriod);
                        }
                    } else {  // 不包含，加入Map
                        newFailFastDBGroups.put(dbGroupName, newPeriod);
                    }
                });
            }
        });
        logger.info(String.format("Fast fail DBGroups may have been updated, old:[%s], new:[%s]",
            failFastDBGroups.keySet().toString(), newFailFastDBGroups.keySet().toString()));
        failFastDBGroups = newFailFastDBGroups;
    }

    /*
     * 判断参数dbGroupName代表的DBGroup是否开启了fast fail
     */
    public static boolean checkDBGroupFastFailOpen(String dbGroupName) {
        boolean ret = false;
        if (failFastDBGroups.containsKey(dbGroupName)) {
            ret = (System.currentTimeMillis() <= failFastDBGroups.get(dbGroupName));
            // 如果map内保存的时间戳已过期，则删除该条记录
            if (!ret) {
                failFastDBGroups.remove(dbGroupName);
                logger.info(String
                    .format("Fast fail DBGroup [%s] has been removed cause of overdue",
                        dbGroupName));
            }
        }
        return ret;
    }
}
