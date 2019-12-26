package me.ele.jarch.athena.allinone;

import me.ele.jarch.athena.DecryptorSpi;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.scheduler.DBChannelDispatcher;
import me.ele.jarch.athena.server.pool.ServerSessionPool;
import me.ele.jarch.athena.sql.seqs.SeqsCache;
import me.ele.jarch.athena.util.deploy.dalgroupcfg.DalGroupConfig;
import me.ele.jarch.athena.util.etrace.MetricFactory;
import org.apache.commons.io.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class AllInOneMonitor {
    private static final Logger logger = LoggerFactory.getLogger(AllInOneMonitor.class);
    private final DecryptorSpi decryptor;
    // store the db info for dbGroup
    private volatile List<DBConnectionInfo> dbConnectionInfos;
    // store the db info for $dal_sequences_user_xxx
    private volatile List<DBConnectionInfo> seqDbConnectionInfos;
    // store the db info for mysqlDBGroup
    private volatile Map<String, DBRole> mysqlDBGroup = new ConcurrentHashMap<>();
    // store the db info for pgDBGroup
    private volatile Map<String, DBRole> pgDBGroup = new ConcurrentHashMap<>();

    private List<ServerSessionPool> inactiveServers = new ArrayList<ServerSessionPool>();

    private static AllInOneMonitor INSTANCE = new AllInOneMonitor();

    public static AllInOneMonitor getInstance() {
        return AllInOneMonitor.INSTANCE;
    }

    private AllInOneMonitor() {
        dbConnectionInfos = new ArrayList<>();
        seqDbConnectionInfos = new ArrayList<>();
        mysqlDBGroup = new HashMap<>();
        pgDBGroup = new HashMap<>();
        decryptor = loadDecryptor();
    }

    private DecryptorSpi loadDecryptor() {
        String decryptorSpiProvider = System
            .getProperty("athena.decryptorspi.provider", "me.ele.jarch.athena.Base64Decryptor");
        for (DecryptorSpi decryptorSpi : ServiceLoader.load(DecryptorSpi.class)) {
            if (decryptorSpiProvider.equals(decryptorSpi.getClass().getName())) {
                decryptorSpi.init();
                return decryptorSpi;
            }
        }
        return null;
    }

    public List<DBConnectionInfo> getDbConnectionInfos() {
        return dbConnectionInfos;
    }

    public List<DBConnectionInfo> getSeqDbConnectionInfos() {
        return seqDbConnectionInfos;
    }

    public Map<String, DBRole> getMysqlDBGroup() {
        return mysqlDBGroup;
    }

    public Map<String, DBRole> getPgDBGroup() {
        return pgDBGroup;
    }

    public synchronized void loadConfig(String configFilePath) {
        try {
            // load db-connection
            doLoadConfig(configFilePath);
        } catch (Exception e) {
            logger.error("Error occur during run AllInOneMonitor", e);
            MetricFactory.newCounter("config.error").once();
        }
    }

    private void doLoadConfig(String configFilePath) throws Exception {
        MetricFactory.newCounter("config.load").once();
        logger.info("Load new allinone DB connection info");
        Map<DBConfType, List<DBConnectionInfo>> newDBInfosMap = parseFromFile(configFilePath);
        updateNormalDbCfgs(newDBInfosMap.getOrDefault(DBConfType.NORMAL, Collections.emptyList()));
        // load $dal_sequence_user config
        updateSeqDbCfgsFromFile(
            newDBInfosMap.getOrDefault(DBConfType.SEQUENCE, Collections.emptyList()));
    }

    private Map<DBConfType, List<DBConnectionInfo>> parseFromFile(String path) throws IOException {
        Map<DBConfType, List<DBConnectionInfo>> result = new HashMap<>();

        try (BufferedReader reader = Files.newBufferedReader(Paths.get(path), Charsets.UTF_8)) {
            reader.lines().forEach(line -> {
                line = line.trim();
                DBConfType confType = DBConfType.getType(line);
                if (confType == DBConfType.NONE) {
                    return;
                }
                DBConnectionInfo info = DBConnectionInfo.parseDBConnectionString(line);
                if (info == null) {
                    return;
                }
                List<DBConnectionInfo> dbInfos =
                    result.computeIfAbsent(confType, (key) -> new ArrayList<DBConnectionInfo>());
                dbInfos.add(info);
            });
        }
        return result;
    }

    private void updateNormalDbCfgs(List<DBConnectionInfo> newDbInfos) {
        if (newDbInfos.size() <= 0) {
            logger.warn("No element is contained in the ALLInOne DB Connection File");
        }
        this.dbConnectionInfos = newDbInfos;
        DBChannelDispatcher.TEMP_DAL_GROUP_CONFIGS.forEach((t, u) -> {
            u.updateDBInfos();
        });
    }

    private void updateSeqDbCfgsFromFile(List<DBConnectionInfo> newSeqDBcfgs) {
        DalGroupConfig dummy = DBChannelDispatcher.TEMP_DAL_GROUP_CONFIGS
            .computeIfAbsent(Constants.DUMMY, DalGroupConfig::new);
        updateDbInfoPerType(dummy, newSeqDBcfgs, DBConfType.SEQUENCE);
    }

    private void updateDbInfoPerType(DalGroupConfig dalGroupConfig, List<DBConnectionInfo> dbInfos,
        DBConfType dbConfType) {
        dalGroupConfig.getDbInfos()
            .removeIf(db -> db.getGroup().startsWith(dbConfType.getSymbol()));
        dalGroupConfig.getDbInfos().addAll(dbInfos);
        dalGroupConfig.write2File();
    }

    private void updateSeqDbCfgs(List<DBConnectionInfo> newSeqDBcfgs) {
        if (Objects.isNull(newSeqDBcfgs)) {
            seqDbConnectionInfos.clear();
            return;
        }
        newSeqDBcfgs.forEach(newCfg -> {
            if (!seqDbConnectionInfos.contains(newCfg)) {
                logger.info("new dal_sequence_user config: " + newCfg);
            }
        });
        if (seqDbConnectionInfos.equals(newSeqDBcfgs)) {
            logger.info("no dal_sequence_user $xxx changed");
            return;
        }
        seqDbConnectionInfos = newSeqDBcfgs;
        SeqsCache.clearAll();
    }

    public void updateSeqDbs(List<DBConnectionInfo> newSeqDBcfgs) {
        Map<DBConfType, List<DBConnectionInfo>> newDBInfosMap = newSeqDBcfgs.stream()
            .collect(Collectors.groupingBy(db -> DBConfType.getType(db.getGroup())));
        this.updateSeqDbCfgs(newDBInfosMap.get(DBConfType.SEQUENCE));
    }

    public void updateDBGroupPerDalGroup(List<DBConnectionInfo> newDbInfos) {
        if (newDbInfos.isEmpty()) {
            logger.warn("No element is contained in the ALLInOne DB Connection File");
        }
        for (DBConnectionInfo newDbInfo : newDbInfos) {
            switch (newDbInfo.getDBVendor()) {
                case MYSQL:
                    if (mysqlDBGroup.getOrDefault(newDbInfo.getGroup(), DBRole.SLAVE)
                        != DBRole.MASTER) {
                        this.mysqlDBGroup.put(newDbInfo.getGroup(), newDbInfo.getDefaultRole());
                    }
                    break;
                case PG:
                    if (pgDBGroup.getOrDefault(newDbInfo.getGroup(), DBRole.SLAVE)
                        != DBRole.MASTER) {
                        this.pgDBGroup.put(newDbInfo.getGroup(), newDbInfo.getDefaultRole());
                    }
                    break;
                default:
                    logger.warn("Unknown db vendor {}, skip updating DBGroup",
                        newDbInfo.getDBVendor());
            }
        }
    }


    public String dec(String txt) {
        return decryptor.decrypt(txt);
    }

    public void cleanInactiveDBConnection() {
        try {
            this.cleanInavtiveServers();
        } catch (Exception e) {
            logger.error("Error occur during process clean inactive db connection", e);
        }
    }

    synchronized public void attachInavtiveServer(ServerSessionPool pool) {
        inactiveServers.add(pool);
    }

    synchronized public void cleanInavtiveServers() {
        List<ServerSessionPool> servers = this.inactiveServers;
        this.inactiveServers = new ArrayList<>();
        for (ServerSessionPool pool : servers) {
            pool.closePoolConnections();
        }
    }
}
