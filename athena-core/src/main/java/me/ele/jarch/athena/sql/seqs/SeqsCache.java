package me.ele.jarch.athena.sql.seqs;

import me.ele.jarch.athena.allinone.AllInOneMonitor;
import me.ele.jarch.athena.allinone.DBConnectionInfo;
import me.ele.jarch.athena.allinone.DBRole;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.scheduler.DBChannelDispatcher;
import me.ele.jarch.athena.sql.seqs.AsyncGlobalID.GID_STATUS;
import me.ele.jarch.athena.util.etrace.MetricFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by xczhang on 15/8/6 上午10:46.
 */
public class SeqsCache {
    private static final Logger logger = LoggerFactory.getLogger(SeqsCache.class);
    private final String seq_name;

    private final String group;
    private volatile SeqsCacheData seqsCacheData = new SeqsCacheData();

    private static List<SeqsCache> cachePool = new CopyOnWriteArrayList<>();

    public SeqsCache(String seq_name, String group) {
        this.seq_name = seq_name;
        this.group = group;
        loadFromGlobalDBConfig();
        cachePool.add(this);
    }

    // close all jdbc db connections and let it reconnect
    public static void clearAll() {
        logger.info("closing all globalid jdbc connections");
        cachePool.forEach(cache -> cache.clear());
    }

    public static void closeConn(DBConnectionInfo dbInfo, boolean keepCache) {
        cachePool.forEach(cache -> {
            if (Objects.equals(dbInfo, cache.seqsCacheData.getSeedSource())) {
                if (keepCache) {
                    cache.closeConnection();
                    logger.error("close globalid jdbc connection {}", dbInfo.getQualifiedDbId());
                } else {
                    cache.clear();
                    logger.error("close globalid jdbc connection {} and clear seqsCacheData",
                        dbInfo.getQualifiedDbId());
                }
            }
        });
    }

    public static void updateGlobalIdCfg() {
        for (SeqsCache cache : cachePool) {
            if (Objects.nonNull(cache.client)) {
                cache.client.updateGlobalId();
            }
        }
    }

    private DBConnectionInfo findDBConnInfo() {
        DBConnectionInfo bizInfo = null, defaultInfo = null, dbMasterInfo = null;
        List<DBConnectionInfo> list = AllInOneMonitor.getInstance().getSeqDbConnectionInfos();
        for (DBConnectionInfo item : list) {
            String group = item.getGroup();
            if (group.equalsIgnoreCase(Constants.DAL_SEQUENCES_USER)) {
                defaultInfo = item;
            } else if (group.equalsIgnoreCase("$" + seq_name)) {
                bizInfo = item;
            }
        }
        // 如果有业务种子库,如$moses,使用之
        if (bizInfo != null) {
            logger.info(String
                .format("[%s] will use biz db [%s:%d] for dal_sequences [%s]", seq_name,
                    bizInfo.getHost(), bizInfo.getPort(), bizInfo.getGroup()));
            return bizInfo;
        }

        // 否则如果有$dal_sequences_user,使用之
        if (defaultInfo != null) {
            logger.info(String
                .format("[%s] will use default [%s:%d] for dal_sequences [%s]", seq_name,
                    defaultInfo.getHost(), defaultInfo.getPort(), defaultInfo.getGroup()));
            return defaultInfo;
        }

        // 否则使用master
        try {
            String dbId = DBChannelDispatcher.getHolders().get(group).getHomeDbGroup()
                .getDBServer(DBRole.MASTER);
            dbMasterInfo = DBChannelDispatcher.getHolders().get(group).getSched(dbId).getInfo();
            logger.info(String
                .format("[%s] will use master db [%s:%d] for dal_sequences [%s]", seq_name,
                    dbMasterInfo.getHost(), dbMasterInfo.getPort(), dbMasterInfo.getGroup()));
            return dbMasterInfo;
        } catch (Exception e) {
            logger.error("", e);
        }
        return null;
    }

    private volatile AsyncGlobalID client = null;

    private Lock lock = new ReentrantLock();

    private void loadFromGlobalDBConfig() {
        if (!lock.tryLock()) {
            return;
        }
        try {
            DBConnectionInfo dbConnectionInfo = findDBConnInfo();
            if (dbConnectionInfo != null) {
                AsyncGlobalID toBeClosed = client;
                client = new AsyncGlobalID(dbConnectionInfo, seq_name, this.seqsCacheData);
                if (toBeClosed != null) {
                    toBeClosed.doQuit("");
                }
                logger.info("usr/pwd : " + dbConnectionInfo.getUser() + "/***");
                MetricFactory.newCounter("config.load").addTag("dbconn", "dal_sequences").once();
            } else {
                logger.error(String.format(
                    "[dal_sequences] Failed to load db connection info for group:[%s], seq_name:[%s]",
                    group, seq_name));
                MetricFactory.newCounter("config.error").addTag("dbconn", "dal_sequences").once();
            }
        } finally {
            lock.unlock();
        }
    }

    public void getSeqValue(SeqsHandler handler) throws Exception {
        if (client == null || client.getAsyncGlobalIDStatus() == GID_STATUS.quit) {
            loadFromGlobalDBConfig();
        }
        AsyncGlobalID asyncGlobalID = client;
        if (Objects.nonNull(asyncGlobalID)) {
            asyncGlobalID.offer(handler);
        } else {
            throw new SeqsException("failed to find Globalid DB ConnInfo from configuration");
        }
    }

    private void closeConnection() {
        AsyncGlobalID toBeClosed = client;
        client = null;
        if (toBeClosed != null) {
            toBeClosed.doQuit("");
        }
    }

    private void clear() {
        this.seqsCacheData = new SeqsCacheData();
        closeConnection();
    }
}
