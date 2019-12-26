package me.ele.jarch.athena.util.rmq;

import me.ele.jarch.athena.allinone.DBConnectionInfo;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.sharding.sql.ShardingSQL;
import me.ele.jarch.athena.sql.CmdQuery;
import me.ele.jarch.athena.sql.QUERY_TYPE;
import me.ele.jarch.athena.util.GreySwitch;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

/**
 * the info sent to rmq
 *
 * @author shaoyang.qi
 */
public class RmqSqlInfo {
    public String originSql = "";
    public String sqlId = "";
    public String sqlType = "";
    public String dbName = "";
    public String ip = "";
    public int port = 0;
    public String dalGroup = "";
    public long affectedRows = -1;
    public boolean isOneKey = false;
    public boolean groupBy = false;
    public boolean orderBy = false;
    public boolean where = false;
    public long singleInValues = -1;
    public long limit = -1;
    public long offset = -1;
    public String table = "";
    public String vendorName = "";
    public Map<String, String> tags = new HashMap<>();

    public static void addTags(SqlSessionContext ctx, String response, String waitTime,
        String execTime) {
        if (Objects.isNull(ctx.rmqSqlInfo)) {
            return;
        }
        RmqSqlInfo info = ctx.rmqSqlInfo;
        info.tags.putAll(ctx.sqlSessionContextUtil.getDalEtraceProducer().getAllDalTags());
        info.tags.put("response", response);
        info.tags.put("sqlWaitTime", waitTime);
        info.tags.put("sqlExecTime", execTime);
        ctx.rmqSqlInfo = info;
    }

    public static void setCommonInfo(SqlSessionContext ctx) {
        if (!GreySwitch.getInstance().isAllowSendAuditSqlToRmq()) {
            return;
        }
        CmdQuery curCmdQuery = ctx.curCmdQuery;
        if (curCmdQuery == null || curCmdQuery.shardingSql == null) {
            return;
        }
        String sqlId = curCmdQuery.getSqlId();
        if (StringUtils.isEmpty(sqlId) || ctx.scheduler == null) {
            return;
        }
        DBConnectionInfo dbInfo = ctx.scheduler.getInfo();
        if (dbInfo == null) {
            return;
        }
        if (!shouldSend(ctx.sqlSessionContextUtil.getProxyDuration(), sqlId,
            curCmdQuery.curQueryType)) {
            return;
        }
        RmqSqlInfo info = new RmqSqlInfo();
        info.sqlId = sqlId;
        info.sqlType = curCmdQuery.curQueryType.name();
        info.dbName = dbInfo.getDatabase();
        info.ip = dbInfo.getHost();
        info.port = dbInfo.getPort();
        info.dalGroup = ctx.getHolder().getDalGroup().getName();
        String originSql = curCmdQuery.query;
        if (ctx.shardingResult != null && ctx.shardingResult.currentResult != null) {
            originSql = ctx.shardingResult.currentResult.shardedSQL;
        }
        info.originSql = originSql;
        info.affectedRows = ctx.sqlSessionContextUtil.getAffectedRows();
        ShardingSQL shardingSQL = curCmdQuery.shardingSql;
        info.isOneKey = shardingSQL.sqlFeature.isOneKey();
        info.groupBy = shardingSQL.sqlFeature.hasGroupBy();
        info.orderBy = shardingSQL.sqlFeature.hasOrderBy();
        info.where = shardingSQL.sqlFeature.hasWhere();
        info.singleInValues = shardingSQL.sqlFeature.getSingleInValues();
        info.limit = shardingSQL.sqlFeature.getLimit();
        info.offset = shardingSQL.sqlFeature.getOffset();
        info.table = shardingSQL.sqlFeature.getTable();
        info.vendorName = shardingSQL.sqlFeature.vendor.name();
        ctx.rmqSqlInfo = info;
    }

    private static boolean shouldSend(long proxyDuration, String sqlId, QUERY_TYPE queryType) {
        int threshold = SendThreshold.getThreshold(proxyDuration);
        if (!AuditSqlToRmq.getInstance().isFirstSend(sqlId)
            && ThreadLocalRandom.current().nextInt(threshold * 10) >= 10) {
            // 不是第一次发送，且概率没有命中
            // 如果是DELETE类型则无条件发送,否则不发送
            return queryType == QUERY_TYPE.DELETE;
        }
        return true;
    }
}
