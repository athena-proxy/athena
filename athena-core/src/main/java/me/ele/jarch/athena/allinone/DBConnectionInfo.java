package me.ele.jarch.athena.allinone;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.util.KVChainParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

@JsonIgnoreProperties({"defaultRole", "alive", "allowAutocommit", "readOnly", "attributesString",
    "pword", "additionalSessionCfg", "additionalDalCfg"}) public class DBConnectionInfo {
    private static final Logger logger = LoggerFactory.getLogger(DBConnectionInfo.class);
    private String group = "";       // dbgroup, not dalgroup
    private String database = "";
    private String host = "";
    private int port;
    private String user = "";
    @JsonProperty("password") private String encodedPasswd = "";
    private String pword = "";
    private DBRole role = DBRole.DUMMY;
    private DBRole defaultRole = DBRole.DUMMY;
    private String id = "";
    private Boolean alive = true;
    private Boolean readOnly = true;
    private Map<String, String> additionalSessionCfg = new TreeMap<>();
    private Map<String, String> additionalDalCfg = new TreeMap<>();
    private String qualifiedDbId = "";
    private String attributesString = "";
    @JsonProperty("attributes") private Map<String, String> attributeMap = Collections.emptyMap();
    @JsonProperty("dbType") private DBVendor dbVendor = DBVendor.MYSQL;
    @JsonProperty("dbEzone") private String ezone = Constants.UNKNOWN;

    public static DBConnectionInfo parseDBConnectionString(String dbConnectionString) {
        DBConnectionInfo info = null;
        String trimLine = dbConnectionString.trim();
        if (trimLine.length() == 0 || trimLine.charAt(0) == '#') {
            return info;
        }
        String[] splitedLine = trimLine.split("\\s+");
        if (splitedLine.length < 8) {
            logger.error("Failed to parseDBConnectionString: " + dbConnectionString);
            return info;
        }
        info = new DBConnectionInfo();
        info.setGroup(splitedLine[0]);
        info.setDatabase(splitedLine[1]);
        info.setHost(splitedLine[2]);
        info.setPort(Integer.valueOf(splitedLine[3]));
        info.setDefaultRole(DBRole.valueOf(splitedLine[4].toUpperCase()));
        info.setRole(info.getDefaultRole());
        info.setId(splitedLine[5]);
        info.setUser(splitedLine[6]);
        info.setEncodedPasswd(splitedLine[7]);
        /*@formatter:off
         * We intentionally set the read-only status of all DB connections to true,
         * which is regarded as the inital state of one DB connection. The reason is
         * for supporting multiple master db configurations in DAL db-connections.cfg.
         * Among these master DB, there must be one in read/write state for daily use,
         * which is guaranteed by DBA. That master DB is called as the "main master DB".
         * The states of these master DB are maintained by the heartbeat mechanism within
         * DAL. That means we need a little time interval for DAL to warmup to find out
         * the main master DB.
         * @formatter:on
         */
        info.setReadOnly(true);
        if (splitedLine.length <= 8) {
            return info;
        }
        info.attributesString = splitedLine[8];
        Map<String, String> attributeMap = KVChainParser.parse(splitedLine[8]);
        String db_vendor = attributeMap.remove(Constants.DB_VENDOR);
        if (db_vendor != null) {
            info.dbVendor = DBVendor.getVendor(db_vendor);
        }
        String ezoneOfDB = attributeMap.remove(Constants.EZONE);
        if (Objects.nonNull(ezoneOfDB)) {
            info.ezone = ezoneOfDB;
        }
        info.attributeMap = attributeMap;
        info.parseAttributes(new HashMap<>(attributeMap));
        return info;
    }

    private void parseAttributes(Map<String, String> attributeMap) {
        // for backend compatibility, just read and abandon it
        attributeMap.remove("allow_autocommit_switch");
        String dal_sequence_table = attributeMap.remove(Constants.DAL_SEQUENCE_TABLE);
        if (dal_sequence_table != null) {
            this.additionalDalCfg.put(Constants.DAL_SEQUENCE_TABLE, dal_sequence_table);
        }
        String dal_sequence_env = attributeMap.remove(Constants.DAL_SEQUENCE_ENV);
        if (Objects.nonNull(dal_sequence_env)) {
            this.additionalDalCfg.put(Constants.DAL_SEQUENCE_ENV, dal_sequence_env);
        }
        String dal_shadow_db = attributeMap.remove(Constants.DAL_SHADOW_DB);
        if (dal_shadow_db != null) {
            this.additionalDalCfg.put(Constants.DAL_SHADOW_DB, dal_shadow_db);
        }
        String pg_db_overdue_in_minutes = attributeMap.remove(Constants.PG_DB_OVERDUE_IN_MINUTES);
        if (pg_db_overdue_in_minutes != null) {
            this.additionalDalCfg.put(Constants.PG_DB_OVERDUE_IN_MINUTES, pg_db_overdue_in_minutes);
        }
        String dist_master = attributeMap.remove(Constants.DIST_MASTER);
        if (dist_master != null) {
            this.additionalDalCfg.put(Constants.DIST_MASTER, dist_master);
        }
        String exDBRole = attributeMap.remove(Constants.EX_ROLE);
        if (Objects.nonNull(exDBRole)) {
            additionalDalCfg.put(Constants.EX_ROLE, exDBRole);
        }
        this.setAdditionalSessionCfg(attributeMap);
    }

    public DBConnectionInfo() {
    }

    public DBConnectionInfo(String group, String database, String host, int port, String user,
        String pword, DBRole role, String id, Boolean active) {
        this(group, database, host, port, user, pword, role, id, active, new TreeMap<>());
    }

    public DBConnectionInfo(String group, String database, String host, int port, String user,
        String pword, DBRole role, String id, Boolean active,
        Map<String, String> additionalSessionCfg) {
        this.group = group;
        this.database = database;
        this.host = host;
        this.port = port;
        this.user = user;
        this.pword = pword;
        this.role = role;
        this.defaultRole = role;
        this.id = id;
        this.setReadOnly(true);
        this.qualifiedDbId = this.group + ":" + this.id;
        this.additionalSessionCfg = additionalSessionCfg;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
        this.qualifiedDbId = this.group + ":" + this.id;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPword() {
        return pword;
    }

    public void setPword(String pword) {
        this.pword = pword;
    }

    public DBRole getRole() {
        return role;
    }

    public void setRole(DBRole role) {
        this.role = role;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
        this.qualifiedDbId = this.group + ":" + this.id;
    }

    public String getAttributesString() {
        return attributesString;
    }

    @JsonIgnore public Boolean isShadowDb() {
        return "true".equalsIgnoreCase(this.additionalDalCfg.get(Constants.DAL_SHADOW_DB));
    }

    public Boolean isAlive() {
        return alive;
    }

    @JsonIgnore public Boolean isActive() {
        if (role == DBRole.MASTER && !isShadowDb()) {
            return alive && !readOnly;
        } else {
            //由于影子DB开关关掉后,会利用将影子DB状态设置成readOnly
            //使其不被选中,因此readOnly也可能为true,但仍认为
            //影子DB是active 状态
            return alive;
        }
    }

    public void setAlive(Boolean alive) {
        this.alive = alive;
    }

    public Boolean isReadOnly() {
        return readOnly;
    }

    public void setReadOnly(Boolean readOnly) {
        this.readOnly = readOnly;
    }

    public Map<String, String> getAdditionalSessionCfg() {
        return new TreeMap<>(additionalSessionCfg);
    }

    public void setAdditionalSessionCfg(Map<String, String> additionalSessionCfg) {
        this.additionalSessionCfg = additionalSessionCfg;
    }

    public DBVendor getDBVendor() {
        return this.dbVendor;
    }

    public void setDBVendor(DBVendor dbVendor) {
        this.dbVendor = dbVendor;
    }

    public String getEzone() {
        return ezone;
    }

    public void setEzone(String ezone) {
        this.ezone = ezone;
    }

    public String getEncodedPasswd() {
        return encodedPasswd;
    }

    public void setEncodedPasswd(String encodedPasswd) {
        this.encodedPasswd = encodedPasswd;
        String decodedPd = AllInOneMonitor.getInstance().dec(encodedPasswd);
        if (decodedPd == null) {
            logger.error("Failed to dec passwd of " + this);
            decodedPd = Constants.DAL_CONFIG_ERR_PWD;
        }
        this.setPword(decodedPd);
    }

    public boolean getDistMatser() {
        return "true"
            .equalsIgnoreCase(this.additionalDalCfg.getOrDefault(Constants.DIST_MASTER, "true"));
    }

    public Map<String, String> getAttributeMap() {
        return attributeMap;
    }

    public void setAttributeMap(Map<String, String> attributeMap) {
        this.attributeMap = attributeMap;
        this.parseAttributes(new HashMap<>(attributeMap));
    }

    @Override public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((alive == null) ? 0 : alive.hashCode());
        result = prime * result + ((database == null) ? 0 : database.hashCode());
        result = prime * result + ((group == null) ? 0 : group.hashCode());
        result = prime * result + ((host == null) ? 0 : host.hashCode());
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + port;
        result = prime * result + ((pword == null) ? 0 : pword.hashCode());
        result = prime * result + ((readOnly == null) ? 0 : readOnly.hashCode());
        result = prime * result + ((role == null) ? 0 : role.hashCode());
        result = prime * result + ((user == null) ? 0 : user.hashCode());
        result = prime * result + ((dbVendor == null) ? 0 : dbVendor.hashCode());
        result = prime * result + ((ezone == null) ? 0 : ezone.hashCode());
        result =
            prime * result + ((additionalSessionCfg == null) ? 0 : additionalSessionCfg.hashCode());
        result = prime * result + ((additionalDalCfg == null) ? 0 : additionalDalCfg.hashCode());
        return result;
    }

    @Override public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        DBConnectionInfo other = (DBConnectionInfo) obj;
        if (alive == null) {
            if (other.alive != null)
                return false;
        } else if (!alive.equals(other.alive))
            return false;
        if (database == null) {
            if (other.database != null)
                return false;
        } else if (!database.equals(other.database))
            return false;
        if (group == null) {
            if (other.group != null)
                return false;
        } else if (!group.equals(other.group))
            return false;
        if (host == null) {
            if (other.host != null)
                return false;
        } else if (!host.equals(other.host))
            return false;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        if (port != other.port)
            return false;
        if (pword == null) {
            if (other.pword != null)
                return false;
        } else if (!pword.equals(other.pword))
            return false;
        if (readOnly == null) {
            if (other.readOnly != null)
                return false;
        } else if (!readOnly.equals(other.readOnly))
            return false;
        if (role != other.role)
            return false;
        if (defaultRole != other.defaultRole)
            return false;
        if (!Objects.equals(ezone, other.ezone))
            return false;
        if (!Objects.equals(dbVendor, other.dbVendor))
            return false;
        if (user == null) {
            if (other.user != null)
                return false;
        } else if (!user.equals(other.user))
            return false;
        if (additionalSessionCfg == null) {
            if (other.additionalSessionCfg != null)
                return false;
        } else if (!additionalSessionCfg.equals(other.additionalSessionCfg))
            return false;
        if (additionalDalCfg == null) {
            if (other.additionalDalCfg != null)
                return false;
        } else if (!additionalDalCfg.equals(other.additionalDalCfg))
            return false;
        return true;
    }

    @Override public String toString() {
        return "DBConnectionInfo [group=" + group + ", id=" + id + ", database=" + database
            + ", host=" + host + ", port=" + port + ", user=" + user + ", pword=***" + ", role="
            + role + ", defaultRole=" + defaultRole + ",  alive=" + alive + ", readOnly=" + readOnly
            + ", additionalSessionCfg=" + additionalSessionCfg + ", additionalDalCfg="
            + additionalDalCfg + "]";
    }

    public String getQualifiedDbId() {
        return qualifiedDbId;
    }

    public Map<String, String> getAdditionalDalCfg() {
        return additionalDalCfg;
    }

    public void setAdditionalDalCfg(Map<String, String> additionalDalCfg) {
        this.additionalDalCfg = additionalDalCfg;
    }

    public void resetStatusByHeartBeatResult(HeartBeatResult hbResult) {
        this.alive = hbResult.isAlive();
        this.readOnly = hbResult.isReadOnly();
    }

    public DBRole getDefaultRole() {
        return defaultRole;
    }

    public void setDefaultRole(DBRole defaultRole) {
        this.defaultRole = defaultRole;
    }

}
