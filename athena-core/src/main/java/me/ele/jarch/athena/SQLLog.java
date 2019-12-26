package me.ele.jarch.athena;

import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.sql.CmdQuery;
import me.ele.jarch.athena.util.GreySwitch;
import me.ele.jarch.athena.util.ResponseStatus;
import me.ele.jarch.athena.util.ResponseStatus.ResponseType;
import me.ele.jarch.athena.util.etrace.EtracePatternUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class SQLLog {
    private static final Logger logger = LoggerFactory.getLogger(SQLLog.class);
    private static final String logMsg = "SQL:[%s] cannot be parsed, please see error.log";

    public static void logWithFilter(String transId, CmdQuery query, String dbInfo,
        String clientAddr, ResponseStatus status, long proxyDur, long serverDur, String proxyTime,
        String shardedTable) {
        if (shouldLog(transId, status)) {
            log(query, dbInfo, clientAddr, status, proxyDur, serverDur, proxyTime, shardedTable);
        }
    }

    public static void log(CmdQuery query, String dbInfo, String clientAddr, ResponseStatus status,
        long proxyDur, long serverDur, String proxyTime, String shardedTable) {
        if (!logger.isInfoEnabled()) {
            return;
        }

        if (status == null) {
            status = new ResponseStatus(ResponseType.OK);
        }

        try {
            StringBuilder sb = new StringBuilder(Constants.TYPICAL_SQL_SIZE);
            if (dbInfo != null) {
                sb.append(dbInfo).append(" => ");
            }
            if (clientAddr != null) {
                sb.append(clientAddr).append(" => ");
            }
            status.print(sb);
            sb.append(" => ");
            sb.append(query.affectedRows).append(" => ");
            if (proxyDur != -1) {
                sb.append("dalDur:").append(proxyDur).append(" => ");
            }
            if (serverDur != -1) {
                sb.append("dbDur:").append(serverDur).append(" => ");
            }
            if (proxyTime != null) {
                sb.append("dalTime: ").append(proxyTime).append(" => ");
            }
            if (query.shardingSql != null) {
                sb.append('[');
                if (UnsafeLog.unsafeLogOn.get() && query.shardingSql.originSQL != null) {
                    sb.append(query.shardingSql.originSQL.replace('\n', ' '));
                } else {
                    sb.append(query.queryComment);
                    sb.append(EtracePatternUtil.addAndGet(query.shardingSql.getMostSafeSQL()).hash);
                    if (!query.shardingSql.getWhiteFields().isEmpty()) {
                        sb.append(' ').append(query.shardingSql.getWhiteFields());
                    }
                }
                sb.append("]");
                //处理log尾部后缀 {shared sql, all_table sharding, sharding, no sharding, batch sql}
                if (Objects.nonNull(shardedTable)) {
                    sb.append(" => ").append(shardedTable);
                    if (query.batchCond.isShuffledByBatchContext()) {
                        sb.append(" local sharded sql");
                    } else {
                        sb.append(" sharded sql");
                    }
                } else if (!query.shardingSql.results.tables.isEmpty()) {
                    sb.append(" => ");
                    query.shardingSql.results.tables.forEach(t -> sb.append(t).append(", "));
                    if (query.shardingSql.results.isAllSharding(query.getShardingRouter(),
                        query.shardingSql.tableNameLowerCase)) {
                        sb.append("all_table ");
                    }
                    sb.append("sharding");
                } else if (!query.shardingSql.getMappingResults().tables.isEmpty()) {
                    sb.append(" => ");
                    query.shardingSql.getMappingResults().tables
                        .forEach(t -> sb.append(t).append(", "));
                    if (query.shardingSql.getMappingResults()
                        .isAllSharding(query.getShardingRouter(),
                            query.shardingSql.tableNameLowerCase)) {
                        sb.append("all_table ");
                    }
                    sb.append("sharding");
                } else if (query.batchCond.isBatchAllowed() && query.batchCond
                    .isNeedBatchAnalyze()) {
                    sb.append(" batch sql");
                } else {
                    sb.append(" no sharding");
                }
            } else {
                // log the original SQL in athena.log
                sb.append(String.format(logMsg, query.query));
                // and log original SQL in error.log
                SQLLogFilter.error(String.format(logMsg, query.query));
            }
            if (UnsafeLog.unsafeLogOn.get()) {
                UnsafeLog.error(sb.toString());
            } else {
                logger.info(sb.toString());
            }
        } catch (Exception e) {
            logger.warn("filter sql failed:" + query.query, e);
        }
    }

    /**
     * 1.记录在事务中或者是autocommit的sql的sql log
     * 2.记录response 状态为非OK的sql的sql log
     *
     * @param transId 当前transaction id
     * @param status
     * @return
     */
    private static boolean shouldLog(String transId, ResponseStatus status) {
        if (GreySwitch.getInstance().isQuerySqlLogEnabled()) {
            return true;
        }
        if (Objects.isNull(transId)) {
            return true;
        }
        if (transId.startsWith(Constants.TRANS_PREFIX)) {
            return true;
        }
        if (transId.startsWith(Constants.AUTOCOMMIT_PREFIX)) {
            return true;
        }
        if (!ResponseType.OK.equals(status.getResponseType())) {
            return true;
        }
        return false;
    }
}
