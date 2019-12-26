package me.ele.jarch.athena.sql;

import com.github.mpjct.jmpjct.mysql.proto.Com_Query;
import me.ele.jarch.athena.constant.Constants;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Created by zhengchao on 16/9/12.
 */
public class BatchQuery {
    private long sequenceId = 0;
    private String query = "";
    private String queryWithoutComment = "";
    private String queryComment = "";
    private Map<String, String> eleMeta = new HashMap<>();
    private String shardTable = "";
    private int shardingIndex = -1;
    private String shardColValue = "";
    private QUERY_TYPE type = QUERY_TYPE.OTHER;

    public void setSequenceId(long sequenceId) {
        if (sequenceId >= 0) {
            this.sequenceId = sequenceId;
        }
    }

    public String getQuery() {
        return query;
    }

    private void setQuery() {
        this.query = this.queryComment + this.queryWithoutComment;
    }

    public void setQueryWithoutComment(String queryWithoutComment) {
        if (StringUtils.isEmpty(queryWithoutComment)) {
            return;
        }
        this.queryWithoutComment = queryWithoutComment;
        setQuery();
    }

    public String getQueryComment() {
        return queryComment;
    }

    private void setQueryComment() {
        putOrRemoveIf(() -> shardingIndex >= 0, Constants.ELE_META_SHARDING_INDEX,
            String.valueOf(shardingIndex));
        putOrRemoveIf(() -> StringUtils.isNotEmpty(this.shardColValue),
            Constants.ELE_META_SHARD_COL_VAL, this.shardColValue);

        StringBuilder sb = new StringBuilder();
        sb.append("/* E:");
        String meta = this.eleMeta.entrySet().stream()
            .map(map -> map.getKey() + "=" + map.getValue().toString())
            .collect(Collectors.joining("&"));
        sb.append(meta);
        sb.append(":E */");
        this.queryComment = sb.toString();
        setQuery();
    }

    private void putOrRemoveIf(Condition condition, String key, String value) {
        if (condition.apply()) {
            this.eleMeta.put(key, value);
        } else {
            this.eleMeta.remove(key);
        }
    }

    public void setEleMeta(Map<String, String> eleMeta) {
        if (Objects.isNull(eleMeta)) {
            return;
        }
        //新建map，保证和外部数据独立
        this.eleMeta = new HashMap<>(eleMeta);
        setQueryComment();
    }

    public void setShardingIndex(int index) {
        this.shardingIndex = index;
        setQueryComment();
    }

    public void resetComment(String groupName, boolean autoCommit4Client, boolean bind2Master) {
        this.eleMeta.put(Constants.ELE_META_DAL_GROUP, groupName);
        this.eleMeta
            .put(Constants.ELE_META_AUTO_COMMIT_FOR_CLIENT, String.valueOf(autoCommit4Client));
        this.eleMeta.put(Constants.ELE_META_BATCH_ANALYZED_MARKER, "true");
        this.eleMeta.put(Constants.ELE_META_BIND_MASTER, String.valueOf(bind2Master));
        setQueryComment();
    }

    public String getShardTable() {
        return shardTable;
    }

    public BatchQuery setShardTable(String shardTable) {
        if (StringUtils.isNotEmpty(shardTable)) {
            this.shardTable = shardTable;
        }
        return this;
    }

    public void setShardColValue(String shardColValue) {
        this.shardColValue = shardColValue;
        setQueryComment();
    }

    public QUERY_TYPE getType() {
        return type;
    }

    public void setType(QUERY_TYPE type) {
        if (Objects.nonNull(type)) {
            this.type = type;
        }
    }

    public byte[] toPacket() {
        Com_Query comQuery = new Com_Query();
        comQuery.sequenceId = this.sequenceId;
        comQuery.setQuery(this.query);
        return comQuery.toPacket();
    }

    @FunctionalInterface private interface Condition {
        public boolean apply();
    }
}
