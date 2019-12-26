package me.ele.jarch.athena.sharding;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.constant.TraceNames;
import me.ele.jarch.athena.scheduler.DBChannelDispatcher;
import me.ele.jarch.athena.sql.seqs.SeqsGenerator;
import me.ele.jarch.athena.sql.seqs.generator.Generator;
import me.ele.jarch.athena.util.etrace.MetricFactory;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.representer.Representer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ShardingConfig {
    private static final Logger logger = LoggerFactory.getLogger(ShardingConfig.class);
    public static final String SHARDING_ID_DELIMITER = ":";
    List<Schema> schemas = new ArrayList<ShardingConfig.Schema>();
    private boolean hasInvalidSchema = false;
    private static final String COMMA = ",";
    public static volatile Map<String, Set<me.ele.jarch.athena.util.deploy.dalgroupcfg.ShardingTable>>
        shardingTableMap = Collections.emptyMap();

    public static ShardingConfig load(String path) throws Exception {
        Representer representer = new Representer();
        representer.getPropertyUtils().setSkipMissingProperties(true);
        Yaml yaml = new Yaml(new Constructor(Schema.class), representer);
        File file = new File(path);
        Iterator<Object> iterator = yaml.loadAll(new FileInputStream(file)).iterator();
        List<Schema> schemaList = new ArrayList<>();
        boolean hasInvalidSchema = false;
        Map<String, Set<me.ele.jarch.athena.util.deploy.dalgroupcfg.ShardingTable>>
            newShardingTableMap = new ConcurrentHashMap<>();

        while (iterator.hasNext()) {
            Schema schema = (Schema) iterator.next();
            try {
                for (Rule rule : schema.getRules()) {
                    rule.parse(file.getParent());
                }
                if (schema.getComposed_key() != null) {
                    schema.getComposed_key().parse(schema.getRules());
                }
                if (schema.getMapping_key() != null) {
                    // mapping_key for first rule
                    schema.getMapping_key().parse(schema.getRules().get(0));
                    // 最外层配置的mapping_key默认与第一个Rule关联
                    schema.getRules().get(0).setMapping_keyIfAbsent(schema.getMapping_key());
                }
                if (schema.validate()) {
                    //多维sharding不支持多composed key,如果发现这种配置,将只保留第一个composed key
                    schema.getComposed_key()
                        .checkMultiShardingAndMultiComposedKey(schema.group, schema.table);
                    schemaList.add(schema);
                    me.ele.jarch.athena.util.deploy.dalgroupcfg.ShardingTable shardingTableCfg =
                        buildShardTableCfg(schema);
                    schema.getGroups().forEach(dalGroup -> {
                        newShardingTableMap.computeIfAbsent(dalGroup, g -> new HashSet<>())
                            .add(shardingTableCfg);
                    });
                } else {
                    hasInvalidSchema = true;
                    logger.error(
                        "failed to validate the sharding schema, group:" + schema.group + " table:"
                            + schema.table);
                    MetricFactory.newCounter("sharding.config.error")
                        .addTag(TraceNames.DALGROUP, schema.group)
                        .addTag(TraceNames.TABLE, schema.table).once();
                }
            } catch (Exception e) {
                hasInvalidSchema = true;
                logger.error(
                    "failed to parse the sharding schema, group:" + schema.group + " table:"
                        + schema.table, e);
                MetricFactory.newCounter("sharding.config.error")
                    .addTag(TraceNames.DALGROUP, schema.group)
                    .addTag(TraceNames.TABLE, schema.table).once();
            }
        }
        ShardingConfig cnf = new ShardingConfig();
        cnf.setSchemas(schemaList);
        cnf.setHasInvalidSchema(hasInvalidSchema);
        shardingTableMap = newShardingTableMap;
        return cnf;
    }

    public static void initShardingCfg() {
        DBChannelDispatcher.TEMP_DAL_GROUP_CONFIGS.forEach((dalGroup, dalGroupConfig) -> {
            if (!Constants.DUMMY.equals(dalGroup)) {
                dalGroupConfig.updateShardingCfg();
            }
        });
    }

    private static me.ele.jarch.athena.util.deploy.dalgroupcfg.ShardingTable buildShardTableCfg(
        Schema schema) {
        me.ele.jarch.athena.util.deploy.dalgroupcfg.ShardingTable shardingTable =
            new me.ele.jarch.athena.util.deploy.dalgroupcfg.ShardingTable();
        shardingTable.setTable(schema.getTable());
        shardingTable.setComposedKey(schema.getComposed_key());
        shardingTable.setRuleForLoading(schema.getRules());
        return shardingTable;
    }

    public List<Schema> getSchemas() {
        return schemas;
    }

    public void setSchemas(List<Schema> schemas) {
        this.schemas = schemas;
    }

    public boolean isHasInvalidSchema() {
        return hasInvalidSchema;
    }

    public void setHasInvalidSchema(boolean hasInvalidSchema) {
        this.hasInvalidSchema = hasInvalidSchema;
    }

    public static class Schema {
        private String group = "";
        private String name = "";
        private Set<String> groups = new HashSet<>();
        private String table = "";
        private List<Rule> rules = new ArrayList<ShardingConfig.Rule>();
        private ComposedKey composed_key;
        @JsonIgnore private MappingKey mapping_key;

        public ComposedKey getComposed_key() {
            return composed_key;
        }

        public boolean validate() {
            return rules.stream().allMatch(rule -> rule.validate());
        }

        public void setComposed_key(ComposedKey composed_key) {
            this.composed_key = composed_key;
        }

        public String getGroup() {
            return group;
        }

        public void setGroup(String group) {
            this.group = group;
            for (String g : group.split(",")) {
                this.groups.add(g);
            }
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getTable() {
            return table;
        }

        public void setTable(String table) {
            this.table = table;
        }

        public List<Rule> getRules() {
            return rules;
        }

        public void setRules(List<Rule> rules) {
            this.rules = rules;
        }

        public Set<String> getGroups() {
            return groups;
        }

        public MappingKey getMapping_key() {
            return mapping_key;
        }

        public void setMapping_key(MappingKey mapping_key) {
            this.mapping_key = mapping_key;
        }

    }


    @JsonIgnoreProperties({"hash_file", "tablePrefix", "expandedDbRoutes", "expandedTablesRoutes"})
    public static class Rule {
        private String column = "";
        @JsonProperty("hashRange") private int hash_range = 1024;
        private String hash_file = "";
        private String tablePrefix = "";
        @JsonProperty("dbRoutes") private Map<String, String> db_routes =
            new HashMap<String, String>();
        @JsonProperty("hashRoutes") private Map<String, Object> tableRoutes =
            new HashMap<String, Object>();
        private Map<String, String> expandedDbRoutes = new HashMap<String, String>();
        @JsonUnwrapped private MappingKey mapping_key;
        private String[] expandedTablesRoutes;

        static private Pattern routePattern = Pattern.compile(
            "\\[((\\s*([0-9]+)\\s*(-\\s*([0-9]+))*\\s*\\,\\s*)+(\\s*([0-9]+)\\s*(-\\s*([0-9]+))*\\s*)|(\\s*([0-9]+)\\s*(-\\s*([0-9]+))*\\s*))]");

        private static Pattern keyPattern = Pattern.compile("\\s*([0-9]+)\\s*-\\s*([0-9]+)\\s*");


        @SuppressWarnings("unchecked") public void parse(String dir) throws FileNotFoundException {
            parseDbRouters();
            Yaml yaml = new Yaml();
            ShardingConfigFileReloader.loader.addConfigFile(dir + File.separator + this.hash_file);
            this.tableRoutes = (Map<String, Object>) yaml
                .load(new FileInputStream(dir + File.separator + this.hash_file));
            parseTableRoutes();
            this.tablePrefix = longestCommonPrefix(this.tableRoutes.keySet());
            // 如果配置了关联的mapping_key,则进行解析
            if (Objects.nonNull(mapping_key)) {
                mapping_key.setColumn(column);
                mapping_key.parse(this);
            }
        }

        public void parseDbRouters() {
            this.db_routes.forEach((tableExpression, dbGroup) -> {
                Matcher m = routePattern.matcher(tableExpression);
                if (!m.find()) {
                    expandedDbRoutes.put(tableExpression, dbGroup);
                    return;
                }
                String scopeString = m.group(1);
                if (StringUtils.isEmpty(scopeString)) {
                    return;
                }
                String[] matchedScopes = scopeString.split(",");
                for (String scope : matchedScopes) {
                    if (!scope.contains("-")) {
                        expandedDbRoutes.put(m.replaceFirst(scope.trim()), dbGroup);
                    } else {
                        String[] intervalKeys = scope.split("-");
                        int start = Integer.valueOf(intervalKeys[0].trim());
                        int end = Integer.valueOf(intervalKeys[1].trim());
                        int step = 1;
                        if (scope.split("-").length > 2) {
                            step = Integer.valueOf(scope.trim().split("-")[2]);
                        }
                        for (int i = start; i <= end; i += step) {
                            String table = m.replaceFirst(String.valueOf(i));
                            if (expandedDbRoutes.containsKey(table)) {
                                logger.error(
                                    "ShardingConfig error, dulplicate table->db routing for table: "
                                        + table + " db:" + dbGroup);
                                MetricFactory.newCounter("sharding.config.error")
                                    .addTag(TraceNames.DBGROUP, dbGroup)
                                    .addTag(TraceNames.TABLE, table).once();
                            }
                            expandedDbRoutes.put(table, dbGroup);
                        }
                    }

                }

            });
        }

        public void parseTableRoutes() {
            expandedTablesRoutes = new String[hash_range];
            tableRoutes.forEach((table, keysObject) -> {
                String keystring = keysObject.toString();
                List<Integer> keys = new ArrayList<Integer>();
                String[] keyItems = keystring.split(",");
                for (String keyItem : keyItems) {
                    Matcher m = keyPattern.matcher(keyItem);
                    if (m.matches()) {
                        int start = Integer.valueOf(m.group(1));
                        int end = Integer.valueOf(m.group(2));
                        for (int i = start; i <= end; i++) {
                            keys.add(i);
                        }
                    } else {
                        keys.add(Integer.valueOf(keyItem.trim()));
                    }
                }
                keys.forEach(key -> {
                    if (expandedTablesRoutes[key] != null) {
                        logger.error(
                            "ShardingConfig error, dulplicate hash->table routing for key " + key
                                + " table:" + table);
                        MetricFactory.newCounter("sharding.config.error")
                            .addTag("key", String.valueOf(key)).addTag(TraceNames.TABLE, table)
                            .once();
                    }
                    expandedTablesRoutes[key] = table;
                });
            });
        }

        public boolean validate() {
            AtomicBoolean ispass = new AtomicBoolean(true);
            for (int i = 0; i < hash_range; i++) {
                String table = expandedTablesRoutes[i];
                if (table == null) {
                    logger.error(
                        this + ". ShardingConfig error, missing hash->table routing for key " + i);
                    ispass.set(false);
                }
            }

            tableRoutes.forEach((table, value) -> {
                String db = expandedDbRoutes.get(table);
                if (db == null) {
                    logger.error(
                        this + ". ShardingConfig error, missing table-db routing for table "
                            + table);
                    ispass.set(false);
                }
            });

            expandedDbRoutes.forEach((table, dbName) -> {
                if (tableRoutes.get(table) == null) {
                    logger.error(this
                        + ". ShardingConfig error, hash->table config not match table-db config ");
                    ispass.set(false);
                }
            });

            return ispass.get();
        }

        public String getHash_file() {
            return hash_file;
        }

        public void setHash_file(String hash_file) {
            this.hash_file = hash_file;
        }

        public String getColumn() {
            return column;
        }

        public void setColumn(String column) {
            this.column = column;
        }

        public int getHash_range() {
            return hash_range;
        }

        public void setHash_range(int hash_range) {
            this.hash_range = hash_range;
        }

        public Map<String, Object> getTableRoutes() {
            return tableRoutes;
        }

        public void setTableRoutes(Map<String, Object> tableRoutes) {
            this.tableRoutes = tableRoutes;
        }

        public Map<String, String> getDb_routes() {
            return db_routes;
        }

        public void setDb_routes(Map<String, String> routes) {
            this.db_routes = routes;
        }

        public String getTablePrefix() {
            return tablePrefix;
        }

        public void setTablePrefix(String tablePrefix) {
            this.tablePrefix = tablePrefix;
        }

        @JsonIgnore public String[] getExpandTablesRoutes() {
            return expandedTablesRoutes;
        }

        @JsonIgnore public Map<String, String> getExpandDbRoutes() {
            return expandedDbRoutes;
        }

        public MappingKey getMapping_key() {
            return mapping_key;
        }

        public void setMapping_key(MappingKey mapping_key) {
            this.mapping_key = mapping_key;
        }

        public MappingKey setMapping_keyIfAbsent(MappingKey mapping_key) {
            if (Objects.isNull(this.mapping_key)) {
                this.mapping_key = mapping_key;
            }
            return this.mapping_key;
        }

        @Override public String toString() {
            final StringBuilder sb = new StringBuilder("Rule{");
            sb.append("column='").append(column).append('\'');
            sb.append(", hash_file='").append(hash_file).append('\'');
            sb.append(", tablePrefix='").append(tablePrefix).append('\'');
            sb.append(", tableRoutes=").append(tableRoutes);
            sb.append(", db_routes=").append(db_routes);
            sb.append(", expandedDbRoutes=").append(expandedDbRoutes);
            sb.append(", mapping_key=").append(mapping_key);
            sb.append(", expandedTablesRoutes=").append(Arrays.toString(expandedTablesRoutes));
            sb.append('}');
            return sb.toString();
        }

        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            Rule rule = (Rule) o;

            if (column != null ? !column.equals(rule.column) : rule.column != null)
                return false;
            if (hash_file != null ? !hash_file.equals(rule.hash_file) : rule.hash_file != null)
                return false;
            if (tablePrefix != null ?
                !tablePrefix.equals(rule.tablePrefix) :
                rule.tablePrefix != null)
                return false;
            if (db_routes != null ? !db_routes.equals(rule.db_routes) : rule.db_routes != null)
                return false;
            if (tableRoutes != null ?
                !tableRoutes.equals(rule.tableRoutes) :
                rule.tableRoutes != null)
                return false;
            if (expandedDbRoutes != null ?
                !expandedDbRoutes.equals(rule.expandedDbRoutes) :
                rule.expandedDbRoutes != null)
                return false;
            if (mapping_key != null ?
                !mapping_key.equals(rule.mapping_key) :
                rule.mapping_key != null)
                return false;
            // Probably incorrect - comparing Object[] arrays with Arrays.equals
            return Arrays.equals(expandedTablesRoutes, rule.expandedTablesRoutes);
        }

        @Override public int hashCode() {
            int result = column != null ? column.hashCode() : 0;
            result = 31 * result + (hash_file != null ? hash_file.hashCode() : 0);
            result = 31 * result + (tablePrefix != null ? tablePrefix.hashCode() : 0);
            result = 31 * result + (db_routes != null ? db_routes.hashCode() : 0);
            result = 31 * result + (tableRoutes != null ? tableRoutes.hashCode() : 0);
            result = 31 * result + (expandedDbRoutes != null ? expandedDbRoutes.hashCode() : 0);
            result = 31 * result + (mapping_key != null ? mapping_key.hashCode() : 0);
            result = 31 * result + Arrays.hashCode(expandedTablesRoutes);
            return result;
        }
    }


    @JsonIgnoreProperties({"columns", "extractors", "shardingColumns", "g"})
    static public class ComposedKey {
        private String column = "";
        private List<String> columns = new ArrayList<>();
        @JsonProperty("seqName") private String seq_name;
        @JsonProperty("composeRules") private List<String> compose_rules = Collections.emptyList();
        private Map<String, ComposedKeyExtractor> extractors =
            new HashMap<String, ShardingConfig.ComposedKeyExtractor>();
        private List<String> shardingColumns = new ArrayList<>();
        private Generator g = null;

        public void parse(List<Rule> rules) throws Exception {
            for (String innerColumn : compose_rules) {
                if (innerColumn.equals(column)) {
                    extractors.put(innerColumn, value -> {
                        return (int) Generator.getHashCode(String.valueOf(value));
                    });
                } else {
                    g = SeqsGenerator.getGenerator(seq_name);
                    extractors.put(innerColumn, value -> {
                        return (int) g.decode(innerColumn, value);
                    });
                }
            }
            rules.forEach(rule -> {
                if (extractors.containsKey(rule.getColumn())) {
                    shardingColumns.add(rule.getColumn());
                }
            });

        }

        public void setExtractors(Map<String, ComposedKeyExtractor> extractors) {
            this.extractors = extractors;
        }

        /**
         * 对传入的sharding相关列的值计算shardingId
         *
         * @param value
         * @return shardingId用于校验事务是否在同一个shardingId上
         */
        public String getShardingId(String value) {
            StringJoiner sj = new StringJoiner(SHARDING_ID_DELIMITER);
            this.shardingColumns.forEach(column -> {
                sj.add(String.valueOf(extractValue(column, value)));
            });
            return sj.toString();
        }

        public List<String> getShardingColumns() {
            return shardingColumns;
        }

        public void setShardingColumns(List<String> shardingColumns) {
            this.shardingColumns = shardingColumns;
        }

        public long extractValue(String column, String value) {
            return extractors.get(column).extractValue(value);
        }

        public Map<String, ComposedKeyExtractor> getExtractors() {
            return extractors;
        }

        public String getColumn() {
            return column;
        }

        /**
         * 获取 Compose_key 集合
         *
         * @return
         */
        public List<String> getColumns() {
            return columns;
        }

        public void checkMultiShardingAndMultiComposedKey(String group, String table) {
            if (this.getShardingColumns().size() > 1 && this.getColumns().size() > 1) {
                logger.error(
                    "multi-dimension sharding not support multi composed key, only first composed key will hold:"
                        + this.columns + " group: " + group + " table: " + table);
                this.columns = this.columns.subList(0, 1);
                MetricFactory.newCounter("sharding.config.error").addTag(TraceNames.DALGROUP, group)
                    .addTag(TraceNames.TABLE, table).once();
            }
        }

        public void setColumn(String column) {
            this.column = column;
            if (StringUtils.isEmpty(column)) {
                return;
            }
            //parse cloumns
            //如果 column 中包含 逗号, 表示配置了多个composed key
            if (this.column.contains(COMMA)) {
                for (String col : this.column.split(COMMA)) {
                    columns.add(col);
                }
            } else {
                columns.add(this.column);
            }
        }

        public void setCompose_rules(List<String> compose_rules) {
            this.compose_rules = compose_rules;
        }

        public String getSeq_name() {
            return seq_name;
        }

        public void setSeq_name(String seq_name) {
            this.seq_name = seq_name;
        }

        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            ComposedKey that = (ComposedKey) o;

            if (column != null ? !column.equals(that.column) : that.column != null)
                return false;
            if (columns != null ? !columns.equals(that.columns) : that.columns != null)
                return false;
            if (seq_name != null ? !seq_name.equals(that.seq_name) : that.seq_name != null)
                return false;
            if (compose_rules != null ?
                !compose_rules.equals(that.compose_rules) :
                that.compose_rules != null)
                return false;
            if (shardingColumns != null ?
                !shardingColumns.equals(that.shardingColumns) :
                that.shardingColumns != null)
                return false;
            return g != null ? g.equals(that.g) : that.g == null;
        }

        @Override public int hashCode() {
            int result = column != null ? column.hashCode() : 0;
            result = 31 * result + (columns != null ? columns.hashCode() : 0);
            result = 31 * result + (seq_name != null ? seq_name.hashCode() : 0);
            result = 31 * result + (compose_rules != null ? compose_rules.hashCode() : 0);
            result = 31 * result + (shardingColumns != null ? shardingColumns.hashCode() : 0);
            result = 31 * result + (g != null ? g.hashCode() : 0);
            return result;
        }
    }


    public static class MappingRule {
        private String composedName;

        private String sepColumnName;

        private String tablePrefix;

        private List<String> originColumns = new ArrayList<>();

        public MappingRule() {

        }

        public MappingRule(String composedName, String sepColumnName, String tablePrefix,
            List<String> originColumns) {
            this.composedName = composedName;
            this.sepColumnName = sepColumnName;
            this.tablePrefix = tablePrefix;
            this.originColumns = originColumns;
        }

        public String getComposedName() {
            return composedName;
        }

        public void setComposedName(String composedName) {
            this.composedName = composedName;
        }

        public String getSepColumnName() {
            return sepColumnName;
        }

        public void setSepColumnName(String columnName) {
            this.sepColumnName = columnName;
        }

        public String getTablePrefix() {
            return tablePrefix;
        }

        public void setTablePrefix(String tablePrefix) {
            this.tablePrefix = tablePrefix;
        }

        public List<String> getOriginColumns() {
            return originColumns;
        }

        public void setOriginColumns(List<String> originColumns) {
            this.originColumns = originColumns;
        }

        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            MappingRule that = (MappingRule) o;
            if (that.getSepColumnName() == null || that.getTablePrefix() == null)
                return false;
            return that.getSepColumnName().equals(this.getSepColumnName()) && that.getTablePrefix()
                .equals(this.getTablePrefix());
        }
    }


    @JsonIgnoreProperties({"column", "replaceTable", "mappingColumns"})
    public static class MappingKey {
        // 映射表中的value,parse方法传入的Rule的column一致
        private String column;
        // 被替换的表前缀
        private String replaceTable = "";
        // 按照sharding.yml配置文件中mapping_rule 书写顺序存储的mappingColumn
        private Map<String, List<String>> mappingColumns = new LinkedHashMap<>();
        private Map<String, MappingRule> mappingRules = new LinkedHashMap<>();
        @JsonProperty("mappingRoutes") private Map<String, String> mapping_rules =
            new LinkedHashMap<>();

        public void parse(Rule rule) {
            if (!Objects.equals(rule.getColumn(), column)) {
                throw new IllegalArgumentException(String.format(
                    "mapping config error, inconsistent rule column: %s with mapping column: %s",
                    rule.getColumn(), column));
            }
            mappingRules.values().forEach((mappingRule) -> {
                mappingColumns.put(mappingRule.getComposedName(), mappingRule.getOriginColumns());
            });
            replaceTable = rule.getTablePrefix();
        }

        public String getColumn() {
            return column;
        }

        public void setColumn(String column) {
            this.column = column;
        }

        public List<MappingRule> getMappingRuleValues() {
            return new ArrayList<>(mappingRules.values());
        }

        public List<MappingRule> getMappingRulesByOneColumnName(String columnName) {
            List<MappingRule> mappingRules = new ArrayList<>();
            if (StringUtils.isEmpty(columnName))
                return mappingRules;
            for (MappingRule mappingRule : this.mappingRules.values()) {
                if (mappingRule.getOriginColumns().contains(columnName)) {
                    mappingRules.add(mappingRule);
                }
            }
            return mappingRules;
        }

        public MappingRule getMappingRuleBySepName(String seqColumnName) {
            if (StringUtils.isEmpty(seqColumnName))
                return new MappingRule();
            for (MappingRule mappingRule : this.mappingRules.values()) {
                if (seqColumnName.equals(mappingRule.getSepColumnName())) {
                    return mappingRule;
                }
            }
            return new MappingRule();
        }

        public boolean containsColumnName(String columnName) {
            if (StringUtils.isEmpty(columnName)) {
                return false;
            }
            AtomicBoolean foundRule = new AtomicBoolean(false);
            mappingRules.values().forEach(mappingRule -> {
                if (mappingRule.getOriginColumns().contains(columnName)) {
                    foundRule.set(true);
                }
            });
            return foundRule.get();
        }

        public boolean containsSeqName(String seqColumnName) {
            if (StringUtils.isEmpty(seqColumnName)) {
                return false;
            }
            AtomicBoolean foundRule = new AtomicBoolean(false);
            mappingRules.values().forEach(mappingRule -> {
                if (seqColumnName.equals(mappingRule.getSepColumnName())) {
                    foundRule.set(true);
                }
            });
            return foundRule.get();
        }

        public void setMapping_rules(Map<String, String> mapping_rules) {
            if (mapping_rules != null) {
                for (Map.Entry entry : mapping_rules.entrySet()) {
                    if (entry.getKey() != null && StringUtils
                        .isNotEmpty(entry.getKey().toString())) {
                        String columns = entry.getKey().toString();
                        String table = entry.getValue().toString();
                        String composedName = columns.replaceAll("\\s*,\\s*", "");
                        MappingRule mr =
                            new MappingRule(composedName, columns.replaceAll("\\s*,\\s*", ","),
                                table, Arrays.asList(columns.split("\\s*,\\s*")));
                        mappingRules.put(composedName, mr);
                    }
                }
            }
            this.mapping_rules = mapping_rules;
        }

        public String getReplaceTable() {
            return replaceTable;
        }

        public Map<String, List<String>> getMappingColumns() {
            return mappingColumns;
        }

        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            MappingKey that = (MappingKey) o;

            if (column != null ? !column.equals(that.column) : that.column != null)
                return false;
            if (mappingColumns != null ?
                !mappingColumns.equals(that.mappingColumns) :
                that.mappingColumns != null)
                return false;
            return mapping_rules != null ?
                mapping_rules.equals(that.mapping_rules) :
                that.mapping_rules == null;
        }

        @Override public int hashCode() {
            int result = column != null ? column.hashCode() : 0;
            result = 31 * result + (replaceTable != null ? replaceTable.hashCode() : 0);
            result = 31 * result + (mappingColumns != null ? mappingColumns.hashCode() : 0);
            result = 31 * result + (mapping_rules != null ? mapping_rules.hashCode() : 0);
            return result;
        }
    }


    @FunctionalInterface static public interface ComposedKeyExtractor {
        public long extractValue(String keyValue);
    }

    static public String longestCommonPrefix(Collection<String> strs) {
        if (strs.isEmpty()) {
            return "";
        }
        String minString = strs.stream().min((s1, s2) -> s1.length() - s2.length()).get();
        for (int i = 0; i < minString.length(); i++) {
            final int index = i;
            if (strs.stream().anyMatch((s -> s.charAt(index) != minString.charAt(index)))) {
                return minString.substring(0, i);
            }
        }
        return minString;
    }
}
