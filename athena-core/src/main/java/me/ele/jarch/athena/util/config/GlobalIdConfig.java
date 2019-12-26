package me.ele.jarch.athena.util.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import me.ele.jarch.athena.scheduler.DBChannelDispatcher;
import me.ele.jarch.athena.sql.seqs.GlobalId;
import me.ele.jarch.athena.util.EnvConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by jinghao.wang on 17/2/20.
 */
public class GlobalIdConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(GlobalIdConfig.class);
    private static final AtomicBoolean ERROR_LOGED_FLAG = new AtomicBoolean(false);
    private List<GlobalIdSchema> schemas = Collections.emptyList();

    public static volatile Map<String, List<GlobalIdSchema>> globalIdSchema =
        Collections.emptyMap();

    public static GlobalIdConfig load(InputStream fileInputStream) {
        Yaml yaml = new Yaml(new Constructor(GlobalIdSchema.class));
        Iterator<Object> iterator = yaml.loadAll(fileInputStream).iterator();
        List<GlobalIdSchema> newGlobalIdSchemas = new ArrayList<>();
        Map<String, List<GlobalIdSchema>> newGlobalIdschemasMap = new TreeMap<>();
        while (iterator.hasNext()) {
            GlobalIdSchema globalIdSchema = (GlobalIdSchema) iterator.next();
            newGlobalIdSchemas.add(globalIdSchema);
            for (Zone zone : globalIdSchema.getZones()) {
                if (zone.isActive()) {
                    for (String group : zone.getGroups()) {
                        newGlobalIdschemasMap.computeIfAbsent(group, (g) -> new ArrayList<>())
                            .add(globalIdSchema);
                    }
                }
            }
        }
        GlobalIdConfig config = new GlobalIdConfig();
        config.setSchemas(newGlobalIdSchemas);
        globalIdSchema = newGlobalIdschemasMap;
        updateDalGroupCfg();
        return config;
    }

    public static void loadCfg(String path) {
        try {
            File file = new File(path);
            if (!file.exists()) {
                if (ERROR_LOGED_FLAG.compareAndSet(false, true)) {
                    LOGGER.error("The file {} dose not exist.", path);
                }
                return;
            }
            if (!file.canRead()) {
                LOGGER.error("The file {} exist, but can not read.", path);
                return;
            }
            GlobalIdConfig config = load(new FileInputStream(file));
            LOGGER.info("load dal-globalids.yml complete, all globalIds: {}, active globalIds: {}",
                config.getSchemas(), GlobalId.allActiveGlobalIds());
        } catch (Exception e) {
            LOGGER.error("load dal-globalids.yml failure", e);
        }
    }

    public List<GlobalIdSchema> getSchemas() {
        return schemas;
    }

    public void setSchemas(List<GlobalIdSchema> schemas) {
        this.schemas = schemas;
    }

    private static void updateDalGroupCfg() {
        DBChannelDispatcher.TEMP_DAL_GROUP_CONFIGS.forEach((dalgroup, config) -> {
            if (!globalIdSchema.containsKey(dalgroup) && config.getGlobalIds().isEmpty()) {
                return;
            }
            List<GlobalIdSchema> newGlobalIDs =
                globalIdSchema.getOrDefault(dalgroup, Collections.emptyList());
            config.setGlobalIds(newGlobalIDs);
            config.write2File();
        });
    }

    public static class GlobalIdSchema {
        /**
         * busniss logic name
         * for composed_seq and common_seq generator
         * biz_name is dal_dual sql where clause `biz = ?`
         * for OrderIdGenerator, OrderIdTestGenerator biz_name
         * is dal_dual sql where clause `seq_name = order_id`
         */
        @JsonProperty("bizName") private String biz_name;
        /**
         * logic seed name
         * for composed_seq and common_seq generator
         * seq_name is dal_dual where clause `biz = ?`
         * for OrderIdGenerator, OrderIdTestGenerator seq_name
         * is real seed name `eleme_order_orderid_seq` and
         * `eleme_order_orderid_seq_test`
         */
        @JsonProperty("seqName") private String seq_name;
        /**
         * 标记globalId是否自动回收
         */
        @JsonProperty private boolean recycled = false;
        @JsonProperty("recycledCacheLifecycle") private int recycled_cache_lifecycle;
        @JsonProperty private String generator;
        @JsonProperty @JsonInclude(JsonInclude.Include.NON_NULL) private String comment;
        @JsonProperty private List<String> params = Collections.emptyList();
        @JsonUnwrapped private Zone activeZone;
        @JsonIgnore private List<Zone> zones = Collections.emptyList();

        public String getBiz_name() {
            return biz_name;
        }

        public void setBiz_name(String biz_name) {
            this.biz_name = biz_name;
        }

        public String getSeq_name() {
            return seq_name;
        }

        public void setSeq_name(String seq_name) {
            this.seq_name = seq_name;
        }

        public String getGenerator() {
            return generator;
        }

        public void setGenerator(String generator) {
            this.generator = generator;
        }

        public boolean getRecycled() {
            return recycled;
        }

        public void setRecycled(boolean recycled) {
            this.recycled = recycled;
        }

        public int getRecycled_cache_lifecycle() {
            return recycled_cache_lifecycle;
        }

        public void setRecycled_cache_lifecycle(int recycled_cache_lifecycle) {
            this.recycled_cache_lifecycle = recycled_cache_lifecycle;
        }

        public List<String> getParams() {
            return params;
        }

        public void setParams(List<String> params) {
            this.params = params;
        }

        public List<Zone> getZones() {
            return zones;
        }

        public void setZones(List<Zone> zones) {
            this.zones = zones;
            this.zones.stream().filter(Zone::isActive).findAny()
                .ifPresent(zone -> activeZone = zone);
        }

        public String getComment() {
            return comment;
        }

        public void setComment(String comment) {
            this.comment = comment;
        }

        public Zone getActiveZone() {
            return activeZone;
        }

        public void setActiveZone(Zone activeZone) {
            this.activeZone = activeZone;
        }

        @Override public String toString() {
            final StringBuilder sb = new StringBuilder("GlobalIdSchema{");
            sb.append("biz_name='").append(biz_name).append('\'');
            sb.append(", seq_name='").append(seq_name).append('\'');
            sb.append(", generator='").append(generator).append('\'');
            sb.append(", params=").append(params);
            sb.append(", zones=").append(zones);
            sb.append('}');
            return sb.toString();
        }

        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            GlobalIdSchema that = (GlobalIdSchema) o;

            if (biz_name != null ? !biz_name.equals(that.biz_name) : that.biz_name != null)
                return false;
            if (seq_name != null ? !seq_name.equals(that.seq_name) : that.seq_name != null)
                return false;
            if (generator != null ? !generator.equals(that.generator) : that.generator != null)
                return false;
            if (params != null ? !params.equals(that.params) : that.params != null)
                return false;
            if (activeZone != null ? !activeZone.equals(that.activeZone) : that.activeZone != null)
                return false;
            return zones != null ? zones.equals(that.zones) : that.zones == null;
        }
    }


    public static class Zone {
        @JsonProperty("idc") private String zone;
        @JsonProperty("seqNameIdc") private String seq_name_zone;
        @JsonProperty("inUse") private boolean in_use;
        @JsonProperty("minSeed") private long min_seed = 0L;
        @JsonProperty("minGlobalId") private long min_globalid = 0L;
        @JsonProperty("maxSeed") private long max_seed = Long.MAX_VALUE;
        @JsonProperty("maxGlobalId") private long max_globalid = Long.MAX_VALUE;
        @JsonIgnore private List<String> groups = new ArrayList<>();

        public void setZone(String zone) {
            this.zone = zone;
        }

        public void setSeq_name_zone(String seq_name_zone) {
            this.seq_name_zone = seq_name_zone;
        }

        public void setIn_use(boolean in_use) {
            this.in_use = in_use;
        }

        public void setGroups(List<String> groups) {
            if (Objects.isNull(groups)) {
                return;
            }
            //snake yaml会把list中的空字符串解析为null装入ArrayList
            //ArrayList不拒绝null值,在此处需要过滤掉null避免使用方产生NPE
            groups.removeIf(Objects::isNull);
            this.groups = groups;
        }

        /**
         * whether globalid is active in current zone
         *
         * @return only when zone is in_use and config file is in right IDC return {@code true},
         * otherwise return {@code false}
         */
        @JsonIgnore public boolean isActive() {
            return in_use && zone.equalsIgnoreCase(EnvConf.get().idc());
        }

        public String getZone() {
            return zone;
        }

        public String getSeq_name_zone() {
            return seq_name_zone;
        }

        public boolean isIn_use() {
            return in_use;
        }

        public long getMin_seed() {
            return min_seed;
        }

        public void setMin_seed(long min_seed) {
            this.min_seed = min_seed;
        }

        public long getMin_globalid() {
            return min_globalid;
        }

        public void setMin_globalid(long min_globalid) {
            this.min_globalid = min_globalid;
        }

        public long getMax_seed() {
            return max_seed;
        }

        public void setMax_seed(long max_seed) {
            this.max_seed = max_seed;
        }

        public long getMax_globalid() {
            return max_globalid;
        }

        public void setMax_globalid(long max_globalid) {
            this.max_globalid = max_globalid;
        }

        public List<String> getGroups() {
            return groups;
        }

        @Override public String toString() {
            final StringBuilder sb = new StringBuilder("Zone{");
            sb.append("zone='").append(zone).append('\'');
            sb.append(", seq_name_zone='").append(seq_name_zone).append('\'');
            sb.append(", in_use=").append(in_use);
            sb.append(", min_seed=").append(min_seed);
            sb.append(", min_globalid=").append(min_globalid);
            sb.append(", max_seed=").append(max_seed);
            sb.append(", max_globalid=").append(max_globalid);
            sb.append(", groups=").append(groups);
            sb.append('}');
            return sb.toString();
        }

        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            Zone zone1 = (Zone) o;

            if (in_use != zone1.in_use)
                return false;
            if (min_seed != zone1.min_seed)
                return false;
            if (min_globalid != zone1.min_globalid)
                return false;
            if (max_seed != zone1.max_seed)
                return false;
            if (max_globalid != zone1.max_globalid)
                return false;
            if (zone != null ? !zone.equals(zone1.zone) : zone1.zone != null)
                return false;
            if (seq_name_zone != null ?
                !seq_name_zone.equals(zone1.seq_name_zone) :
                zone1.seq_name_zone != null)
                return false;
            return groups != null ? groups.equals(zone1.groups) : zone1.groups == null;
        }

        @Override public int hashCode() {
            int result = zone != null ? zone.hashCode() : 0;
            result = 31 * result + (seq_name_zone != null ? seq_name_zone.hashCode() : 0);
            result = 31 * result + (in_use ? 1 : 0);
            result = 31 * result + (int) (min_seed ^ (min_seed >>> 32));
            result = 31 * result + (int) (min_globalid ^ (min_globalid >>> 32));
            result = 31 * result + (int) (max_seed ^ (max_seed >>> 32));
            result = 31 * result + (int) (max_globalid ^ (max_globalid >>> 32));
            result = 31 * result + (groups != null ? groups.hashCode() : 0);
            return result;
        }
    }
}
