package me.ele.jarch.athena.allinone;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import me.ele.jarch.athena.server.async.HeartBeat;
import me.ele.jarch.athena.server.async.MasterHeartBeat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * 存放HeartBeat和DBGroup映射关系, 线程不安全。
 */
public class HeartBeatHolder {
    private static final Logger logger = LoggerFactory.getLogger(HeartBeatHolder.class);

    private final MasterHeartBeat heartBeat;

    private final Multiset<DBGroup> dbGroups;


    public HeartBeatHolder(DBConnectionInfo dbInfo) {
        dbGroups = HashMultiset.create();
        heartBeat = new MasterHeartBeat(dbInfo);
        heartBeat.start();
    }

    public MasterHeartBeat startHeartBeat(DBGroup dbGroup) {
        if (dbGroups.size() > 10_000) {
            logger.error("dbGroup size in Heartbeat {} has reached 10000, skip adding dbgroup {}",
                heartBeat, dbGroup);
            return heartBeat;
        }
        dbGroups.add(dbGroup);
        return heartBeat;
    }

    public void stopHeartBeat(DBGroup dbGroup) {
        dbGroups.remove(dbGroup);
    }

    public Set<DBGroup> getDbGroups() {
        return new HashSet<>(dbGroups);
    }

    public HeartBeat getHeartBeat() {
        return heartBeat;
    }
}
