package me.ele.jarch.athena.util.deploy.dalgroupcfg;

import me.ele.jarch.athena.sharding.ShardingConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

public class ShardingTable {
    private static final Logger logger = LoggerFactory.getLogger(ShardingTable.class);

    private String table;
    private ShardingConfig.ComposedKey composedKey;
    private List<ShardingConfig.Rule> rules = null;

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public ShardingConfig.ComposedKey getComposedKey() {
        return composedKey;
    }

    public void setComposedKey(ShardingConfig.ComposedKey composedKey) {
        this.composedKey = composedKey;
    }

    public List<ShardingConfig.Rule> getRules() {
        return rules;
    }

    public void setRules(List<ShardingConfig.Rule> rules) {
        this.rules = rules;
        rules.forEach(rule -> {
            rule.parseDbRouters();
            rule.parseTableRoutes();
            rule.setTablePrefix(ShardingConfig.longestCommonPrefix(rule.getTableRoutes().keySet()));
            if (Objects.nonNull(rule.getMapping_key()) && !rule.getMapping_key()
                .getMappingRuleValues().isEmpty()) {
                rule.getMapping_key().setColumn(rule.getColumn());
                rule.getMapping_key().parse(rule);
            }
        });
        if (Objects.nonNull(composedKey)) {
            try {
                composedKey.parse(rules);
            } catch (Exception e) {
                logger.error("failed to parse composed key rules!", e);
            }
        }
    }

    public void setRuleForLoading(List<ShardingConfig.Rule> rules) {
        this.rules = rules;
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        ShardingTable that = (ShardingTable) o;

        if (table != null ? !table.equals(that.table) : that.table != null)
            return false;
        if (composedKey != null ? !composedKey.equals(that.composedKey) : that.composedKey != null)
            return false;
        return rules != null ? rules.equals(that.rules) : that.rules == null;
    }

    @Override public int hashCode() {
        int result = table != null ? table.hashCode() : 0;
        result = 31 * result + (composedKey != null ? composedKey.hashCode() : 0);
        result = 31 * result + (rules != null ? rules.hashCode() : 0);
        return result;
    }
}
