package me.ele.jarch.athena.sql;

import com.github.mpjct.jmpjct.mysql.proto.Com_Initdb;
import com.github.mpjct.jmpjct.mysql.proto.Com_Query;
import com.github.mpjct.jmpjct.mysql.proto.Flags;
import com.github.mpjct.jmpjct.mysql.proto.Packet;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.constant.TraceNames;
import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.sharding.ShardingRouter;
import me.ele.jarch.athena.sharding.sql.Send2BatchCond;
import me.ele.jarch.athena.sharding.sql.ShardingSQL;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

public class CmdQuery extends ComposableCommand {

    private static final Logger logger = LoggerFactory.getLogger(CmdQuery.class);

    public static final String PING_SQL = "SELECT dal_fake_mysql_ping()";
    public static final String SYNC_SQL = "SELECT dal_fake_pg_sync()";
    public static final String EMPTY_SQL = "SELECT dal_fake_pg_empty_query()";

    /**
     * 当Druid无法解析该SQL时,会取该SQL前32个字符,并在头部加上此前缀
     * <p>
     * 以此方便grep追踪哪些SQL未解析成功
     */
    private static final String PATTERN_PARSE_FAIL_PREFIX = "[dal_parse_fail] ";

    long sequenceId;
    public byte type;
    protected String typeName;
    public String schema = "";
    public String query = "";
    public String queryComment = "";
    public String queryWithoutComment = "";
    // 当前SQL类型,为了mapping etrace追踪
    public QUERY_TYPE curQueryType = QUERY_TYPE.OTHER;
    // 原始SQL类型
    public QUERY_TYPE queryType = QUERY_TYPE.OTHER;
    public boolean isBindMasterExpr = false;
    public boolean needCacheResult = false;
    // 该字段会在sharding情况下被修改
    public String table;
    // sql里指定的db，没有输入时为空
    public String specifyDB = "";

    // 原始表名,不会被修改
    public String originalTable;
    public Map<String, String> metaMap;
    public String rid = null;
    public String rpcid = null;
    public String appid = null;
    public String shardKey = null;
    public String shardValue = null;
    public String shardKeyAndValue = null;
    public long affectedRows = -1;
    private boolean grayRead = false;
    public Send2BatchCond batchCond = Send2BatchCond.getDefault();
    protected ShardingRouter shardingRouter = ShardingRouter.defaultShardingRouter;
    protected final String dalGroup;
    private Predicate<String> whiteFieldsFilter = ShardingSQL.BLACK_FIELDS_FILTER;
    private String indexHints = "";

    public ShardingSQL shardingSql;

    protected byte[] packet;
    protected byte[] sendBuf;
    /**
     * client发来的原始sql的sqlId
     */
    protected String sqlId = "";
    /**
     * client发来的原始sql的sqlPattern
     */
    protected String sqlPattern = "";

    public byte[] getSendBuf() {
        return sendBuf;
    }

    protected static String mapGet(Map<String, String> map, String key) {
        if (map == null) {
            return null;
        }
        return map.get(key);
    }

    public CmdQuery(byte[] packet, String dalGroup) {
        this.packet = packet;
        this.dalGroup = dalGroup;
    }

    @Override protected boolean doInnerExecute() throws QuitException {
        sequenceId = Packet.getSequenceId(packet);
        type = Packet.getType(packet);
        typeName = Packet.getPacketTypeName(type);

        if (type == Flags.COM_STMT_PREPARE) {
            query = Com_Query.loadFromPacket(packet).getQuery();
            queryType = QUERY_TYPE.JDBC_PREPARE_STMT;
            return false;
        }

        // COM_PING有特殊逻辑,伪造Com_Query数据结构,以此生成相关SafeSQL等信息,并替换其QUERY_TYPE
        if (type == Flags.COM_PING) {
            Com_Query fakeQuery = new Com_Query();
            fakeQuery.setQuery(PING_SQL);
            handleQuery(fakeQuery);
            queryType = QUERY_TYPE.MYSQL_PING;
            return false;
        }

        // COM_QUERY has specific logic
        if (type != Flags.COM_QUERY) {
            if (type != Flags.COM_FIELD_LIST) {
                logger.error("Other SQL Type: " + typeName + " " + type);
            }
            sendBuf = packet;
        }

        switch (type) {
            case Flags.COM_QUIT:
                return true;

            // Extract out the new default schema
            case Flags.COM_INIT_DB:
                schema = Com_Initdb.loadFromPacket(packet).schema;
                logger.error("USE " + schema + " found, it's danger");
                queryType = QUERY_TYPE.IGNORE_CMD;
                break;
            case Flags.COM_QUERY:
                Com_Query comQuery = Com_Query.loadFromPacket(packet);
                handleQuery(comQuery);
                break;
            default:
                break;
        }
        return true;
    }

    protected void handleQuery(QueryPacket comQuery) throws QuitException {
        query = comQuery.getQuery();
        EleMetaParser parser = EleMetaParser.parse(query);
        queryComment = parser.getQueryComment();
        queryWithoutComment = parser.getQueryWithoutComment();
        metaMap = parser.getEleMeta();

        rid = mapGet(metaMap, "rid");
        rpcid = mapGet(metaMap, "rpcid");
        appid = mapGet(metaMap, "appid");
        shardKey = mapGet(metaMap, "shardkey");
        shardValue = mapGet(metaMap, "shardvalue");
        shardKeyAndValue = shardKey == null && shardValue == null ?
            null :
            String.format("%s=%s", shardKey, shardValue);
        try {
            shardingSql =
                handleSQLBasicly(queryWithoutComment, queryComment, shardingRouter, batchCond,
                    whiteFieldsFilter);
            isBindMasterExpr = shardingSql.isContainsBindMasterExpr;
            needCacheResult = shardingSql.needCacheResult;
            curQueryType = queryType = shardingSql.queryType;
            originalTable = table = shardingSql.tableNameLowerCase;
            specifyDB = shardingSql.specifyDB;
            sqlPattern = shardingSql.getOriginMostSafeSQL();
            sqlId = DigestUtils.md5Hex(sqlPattern);
            if (shardingSql.needRewriteSQLWhenNoSharding()) {
                query = shardingSql.originSQL;
                comQuery.setQuery(query);
            }
            ShardingSQL.handleSQLSharding(shardingSql);
        } catch (Exception e) {
            if (shardingSql == null) {
                sqlPattern = PATTERN_PARSE_FAIL_PREFIX + (queryWithoutComment.length() > 32 ?
                    queryWithoutComment.substring(0, 32) :
                    queryWithoutComment);
                sqlId = DigestUtils.md5Hex(sqlPattern);
                // 这个query不符合普通SQL语法规范,但仍然发往DB
                logger.error("send SQL to DB even if SQL is invalid, SQL:[" + query + "]", e);
            } else {
                // 这条SQL符合普通SQL语法规范,但不符合sharding规范,报错,抛异常
                throw e;
            }
        }
        if (Objects.isNull(shardingSql)) {
            // 如果无法分析正确分析SQL语法，则不改写原始SQL字节，原样发送到从库
            sendBuf = packet;
            return;
        }
        StringBuilder sb = new StringBuilder(Constants.TYPICAL_COMMENT_SIZE);
        sb.append(queryComment);
        appendMeta(sb);
        queryComment = sb.toString();
        QueryPacket queryPacket = newQueryPacket();
        shardingSql.setComment(queryComment);
        // 此路径下shardingSql.originSQL约等于queryWithoutComment, 只有在Show 类型SQL时queryWithoutComment会被改写
        queryPacket.setQuery(queryComment + shardingSql.originSQL);
        sendBuf = queryPacket.toPacket();
    }

    protected QueryPacket newQueryPacket() {
        return new Com_Query();
    }

    private void appendMeta(StringBuilder sb) {
        sb.append("/*E:");
        sb.append("group").append('=').append(dalGroup);
        sb.append('&');
        sb.append(TraceNames.SQL_ID).append('=').append(sqlId);
        sb.append(":E*/");
    }

    protected ShardingSQL handleSQLBasicly(String queryWithoutComment, String queryComment,
        ShardingRouter shardingRouter, Send2BatchCond batchCond,
        Predicate<String> whiteFieldsFilter) {
        return ShardingSQL
            .handleMySQLBasicly(queryWithoutComment, queryComment, shardingRouter, batchCond,
                whiteFieldsFilter);
    }

    public void rewriteBytes(byte[] bytes) {
        sendBuf = bytes;
    }

    public boolean isSharded() {
        return shardingSql != null && (shardingSql.results.count != 0
            || shardingSql.getMappingResults().count != 0);
    }

    public boolean hasMappingShardingSql() {
        return shardingSql != null && shardingSql.getMappingResults().hasNext();
    }

    public String getTypeName() {
        return typeName;
    }

    public boolean isGrayRead() {
        return grayRead;
    }

    public void setGrayRead(boolean grayRead) {
        this.grayRead = grayRead;
    }

    public ShardingRouter getShardingRouter() {
        return shardingRouter;
    }

    public void setShardingRouter(ShardingRouter shardingRouter) {
        this.shardingRouter = shardingRouter;
    }

    public String getSqlId() {
        return sqlId;
    }

    public String getSqlPattern() {
        return sqlPattern;
    }

    public void setWhiteFieldsFilter(Predicate<String> whiteFieldsFilter) {
        this.whiteFieldsFilter = whiteFieldsFilter;
    }

    public void setIndexHints(String indexHints) {
        this.indexHints = indexHints;
    }

    public String getIndexHints() {
        return indexHints;
    }
}
