package me.ele.jarch.athena.util.deploy.dalgroupcfg;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.ObjectMapper;
import me.ele.jarch.athena.allinone.AllInOneMonitor;
import me.ele.jarch.athena.allinone.DBConnectionInfo;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.constant.Metrics;
import me.ele.jarch.athena.constant.TraceNames;
import me.ele.jarch.athena.netty.upgrade.UpgradeCenter;
import me.ele.jarch.athena.sharding.ShardingConfig;
import me.ele.jarch.athena.sql.seqs.GlobalId;
import me.ele.jarch.athena.util.CredentialsCfg;
import me.ele.jarch.athena.util.JacksonObjectMappers;
import me.ele.jarch.athena.util.NoThrow;
import me.ele.jarch.athena.util.UserInfo;
import me.ele.jarch.athena.util.config.GlobalIdConfig;
import me.ele.jarch.athena.util.deploy.DALGroup;
import me.ele.jarch.athena.util.etrace.MetricFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

@JsonPropertyOrder({"name", "id", "status", "dbType", "username", "password", "readonlyUser",
    "batchAllowed", "pureSlaveOnly", "slaveSelectStrategy", "mlType", "homeDbGroupName", "offline",
    "dbGroups", "shardings", "globalIds"}) @JsonIgnoreProperties({"autocommit"})
public class DalGroupConfig {
    private static Logger logger = LoggerFactory.getLogger(DalGroupConfig.class);

    public static final DalGroupConfig DUMMY = new DalGroupConfig();

    private String name;
    private String id = "-1";
    private String status = "verified";
    private String dbType = "MYSQL";
    private String username = "";
    private String password = "";
    private boolean readonlyUser = false;
    private boolean batchAllowed;
    private boolean pureSlaveOnly = false;
    private String slaveSelectStrategy = "default";
    private MlType mlType = MlType.UNKNOWN;
    private boolean offline = false;
    private String homeDbGroupName;
    @JsonProperty("dbGroups") private volatile List<DBConnectionInfo> dbInfos = new ArrayList<>();
    @JsonProperty("shardings") private Set<ShardingTable> shardingTables = new HashSet<>();
    private List<GlobalIdConfig.GlobalIdSchema> globalIds = new ArrayList<>();
    @JsonIgnore public volatile Set<String> dbGroups = new HashSet<>();
    @JsonIgnore private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    public DalGroupConfig() {
    }

    public DalGroupConfig(String name) {
        this.name = name;
    }

    public synchronized void updateConfigs(DALGroup dalGroup) {
        this.updateByDalgroup(dalGroup);
        this.updateShardingCfg();
        this.updateDBInfos();
        this.updateGlobalIds();
    }

    public synchronized void updateByDalgroup(DALGroup dalGroup) {
        this.setName(dalGroup.getName());
        this.setHomeDbGroupName(dalGroup.getDbGroup());
        this.setBatchAllowed(dalGroup.isBatchAllowed());
        this.pureSlaveOnly = dalGroup.isPureSlaveOnly();
        this.slaveSelectStrategy = dalGroup.getSlaveSelectStrategy();
        this.setDalgroupOffline(dalGroup.isOffline());
        updateCredentials(dalGroup.getName());
        this.updateDBInfos();
        this.write2File();
    }

    public void updateCredentials(String dalGroup) {
        UserInfo userInfo = CredentialsCfg.getConfig().entrySet().stream().filter(
            entry -> entry.getKey().contains("@") && dalGroup.equals(entry.getKey().split("@")[1])
                && !entry.getKey().contains("_osp_")).map(Map.Entry::getValue).findFirst()
            .orElseGet(() -> CredentialsCfg.getConfig().entrySet().stream()
                .filter(e -> e.getKey().startsWith(dalGroup) && !e.getKey().contains("_osp_"))
                .map(Map.Entry::getValue).findFirst().orElse(null));
        if (Objects.nonNull(userInfo)) {
            this.setUsername(userInfo.getName());
            this.setPassword(userInfo.getEncryptedPwd());
            this.readonlyUser = userInfo.isReadOnly();
            this.write2File();
        }
    }

    private boolean isInScope(String dbGroup) {
        if (dbGroup == null) {
            return false;
        }
        if (dbGroup.equals(this.homeDbGroupName)) {
            return true;
        }
        List<String> shardingDBGroups =
            this.shardingTables.stream().map(ShardingTable::getRules).flatMap(Collection::stream)
                .map(rule -> rule.getDb_routes().values()).flatMap(Collection::stream).distinct()
                .collect(Collectors.toList());
        return shardingDBGroups.contains(dbGroup);
    }

    public synchronized void updateShardingCfg() {
        Set<ShardingTable> newSTable =
            ShardingConfig.shardingTableMap.getOrDefault(this.name, Collections.emptySet());
        this.setShardingTables(newSTable);
        this.updateDBInfos();
        this.write2File();
    }

    public synchronized void updateDBInfos() {
        List<DBConnectionInfo> dbConnectionInfos = new ArrayList<>();
        AllInOneMonitor.getInstance().getDbConnectionInfos().forEach(info -> {
            if (this.isInScope(info.getGroup())) {
                dbConnectionInfos.add(info);
            }
        });
        this.setDbInfos(dbConnectionInfos);
        if (!dbConnectionInfos.isEmpty())
            this.setDbType(String.valueOf(dbConnectionInfos.get(0).getDBVendor()));
        this.write2File();
    }

    public synchronized void updateGlobalIds() {
        GlobalIdConfig.globalIdSchema.entrySet().stream().filter(e -> e.getKey().equals(name))
            .findAny().ifPresent(e -> {
            this.globalIds = e.getValue();
            this.write2File();
        });
    }

    private void clearDalGroupResources() {
        NoThrow.call(() -> {
            //            AllInOne.getInstance().removeDeletedDbs(this);
            ShardingConfig.shardingTableMap.remove(this.name);
            GlobalId.removeDalGroupGlobalId(this);
        });
    }

    public synchronized void write2File() {
        //skip writting to file as current process is downgrading
        if (UpgradeCenter.getInstance().isDowngradeTriggered()) {
            return;
        }
        if (offline) {
            logger.info("dalgroup {} is offline, skip writing to file", name);
            return;
        }
        Path dir = Paths.get(Constants.DALGROUP_CFG_FOLDER);
        try {
            if (!Files.exists(dir)) {
                Files.createDirectories(dir);
            }
            Path filePath =
                Paths.get(Constants.DALGROUP_CFG_FOLDER, "dalgroup-" + this.getName() + ".json");
            logger.info("try to write to file {}", filePath.getFileName());
            ObjectMapper mapper = JacksonObjectMappers.getPrettyMapper();
            mapper.writeValue(filePath.toFile(), this);
        } catch (Exception e) {
            logger.error("Failed writing dalGroup {} JSON to file", this.getName(), e);
            MetricFactory.newCounter(Metrics.DALGROUP_CONFIG_WRITE_FAILED)
                .addTag(TraceNames.DALGROUP, this.getName());
        }
    }

    public synchronized void deleteFile() {
        Path filePath =
            Paths.get(Constants.DALGROUP_CFG_FOLDER, "dalgroup-" + this.getName() + ".json");
        logger.info("Try to delete file {}", filePath.getFileName());
        if (Files.exists(filePath)) {
            try {
                Files.delete(filePath);
            } catch (Exception e) {
                logger.error("Error when delete dalgroup {} JSON file", this.getName());
            }
        }
    }

    /**
     * 用于判断当前dbInfos和更新后的是否一致。 仅判断dbinfo的配置参数而不去检查启动后会修改的状态参数
     *
     * @param previous
     * @param current
     * @return
     */
    private boolean isDBInfosChanged(List<DBConnectionInfo> previous,
        List<DBConnectionInfo> current) {
        if (Objects.isNull(previous) || Objects.isNull(current)) {
            return true;
        }
        if (previous.size() != current.size()) {
            return true;
        }
        return !previous.stream()
            .allMatch(old -> current.stream().anyMatch(newInfo -> isSameDbInfo(old, newInfo)));
    }

    private boolean isSameDbInfo(DBConnectionInfo db1, DBConnectionInfo db2) {
        if (db1 == db2)
            return true;
        if (Objects.isNull(db1) || Objects.isNull(db2))
            return false;
        if (!db1.getDatabase().equals(db2.getDatabase()))
            return false;
        if (!db1.getGroup().equals(db2.getGroup()))
            return false;
        if (!db1.getHost().equals(db2.getHost()))
            return false;
        if (!db1.getId().equals(db2.getId()))
            return false;
        if (db1.getPort() != db2.getPort())
            return false;
        if (!db1.getPword().equals(db2.getPword()))
            return false;
        if (db1.getRole() != db2.getRole())
            return false;
        if (!db1.getUser().equals(db2.getUser()))
            return false;
        if (!Objects.equals(db1.getEzone(), db2.getEzone()))
            return false;
        if (db1.getDBVendor() != db2.getDBVendor())
            return false;
        if (!db1.getAdditionalSessionCfg().equals(db2.getAdditionalSessionCfg()))
            return false;
        if (!db1.getAdditionalDalCfg().equals(db2.getAdditionalDalCfg()))
            return false;
        return true;
    }

    public boolean getReadonlyUser() {
        return readonlyUser;
    }

    public void setReadonlyUser(boolean readonlyUser) {
        this.readonlyUser = readonlyUser;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDbType() {
        return dbType;
    }

    public void setDbType(String dbType) {
        this.dbType = dbType;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public boolean getBatchAllowed() {
        return batchAllowed;
    }

    public void setBatchAllowed(boolean batchAllowed) {
        this.batchAllowed = batchAllowed;
    }

    public boolean isPureSlaveOnly() {
        return pureSlaveOnly;
    }

    public void setPureSlaveOnly(boolean pureSlaveOnly) {
        this.pureSlaveOnly = pureSlaveOnly;
    }

    public String getSlaveSelectStrategy() {
        return slaveSelectStrategy;
    }

    public void setSlaveSelectStrategy(String slaveSelectStrategy) {
        this.slaveSelectStrategy = slaveSelectStrategy;
    }

    public boolean getOffline() {
        return offline;
    }

    public void setOffline(boolean offline) {
        this.offline = offline;
    }

    public void setDalgroupOffline(boolean offline) {
        this.offline = offline;
        //if the dalgroup is set to offline, clear all the resources
        if (offline) {
            clearDalGroupResources();
        }
    }

    public String getHomeDbGroupName() {
        return homeDbGroupName;
    }

    public void setHomeDbGroupName(String homeDbGroupName) {
        this.homeDbGroupName = homeDbGroupName;
    }

    public List<DBConnectionInfo> getDbInfos() {
        return dbInfos;
    }

    public void setDbInfos(List<DBConnectionInfo> dbInfos) {
        this.dbInfos = dbInfos;
        Set<String> newDbGroups = new HashSet<>();
        dbInfos.forEach(dbConnectionInfo -> {
            newDbGroups.add(dbConnectionInfo.getGroup());
            dbConnectionInfo.setDefaultRole(dbConnectionInfo.getRole());
        });
        this.dbGroups = newDbGroups;
    }

    public Set<ShardingTable> getShardingTables() {
        return shardingTables;
    }

    public void setShardingTables(Set<ShardingTable> shardingTables) {
        this.shardingTables = shardingTables;
    }

    public List<GlobalIdConfig.GlobalIdSchema> getGlobalIds() {
        return globalIds;
    }

    public void setGlobalIds(List<GlobalIdConfig.GlobalIdSchema> globalIds) {
        this.globalIds = globalIds;
        globalIds.stream().map(GlobalIdConfig.GlobalIdSchema::getActiveZone)
            .forEach(zone -> zone.getGroups().add(name));
    }

    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public MlType getMlType() {
        return mlType;
    }

    public void setMlType(MlType mlType) {
        this.mlType = mlType;
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        DalGroupConfig that = (DalGroupConfig) o;
        if (Objects.nonNull(dbInfos) ?
            isDBInfosChanged(dbInfos, that.dbInfos) :
            Objects.nonNull(that.dbInfos)) {
            return false;
        }
        return readonlyUser == that.readonlyUser && batchAllowed == that.batchAllowed
            && pureSlaveOnly == that.pureSlaveOnly && offline == that.offline && Objects
            .equals(name, that.name) && Objects.equals(id, that.id) && Objects
            .equals(status, that.status) && Objects.equals(dbType, that.dbType) && Objects
            .equals(username, that.username) && Objects.equals(password, that.password) && Objects
            .equals(slaveSelectStrategy, that.slaveSelectStrategy) && mlType == that.mlType
            && Objects.equals(homeDbGroupName, that.homeDbGroupName) && Objects
            .equals(dbInfos, that.dbInfos) && Objects.equals(shardingTables, that.shardingTables)
            && Objects.equals(globalIds, that.globalIds) && Objects.equals(dbGroups, that.dbGroups)
            && Objects.equals(additionalProperties, that.additionalProperties);
    }

    @Override public int hashCode() {
        return Objects
            .hash(name, id, status, dbType, username, password, readonlyUser, batchAllowed,
                pureSlaveOnly, slaveSelectStrategy, mlType, offline, homeDbGroupName, dbInfos,
                shardingTables, globalIds, dbGroups, additionalProperties);
    }
}
