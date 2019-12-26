package me.ele.jarch.athena.netty.state;

import com.github.mpjct.jmpjct.mysql.proto.Com_Query;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.netty.BatchSessionContext;
import me.ele.jarch.athena.sharding.sql.ShardingBatchSQL;
import me.ele.jarch.athena.sql.BatchQuery;
import me.ele.jarch.athena.sql.EleMetaParser;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Created by zhengchao on 16/8/29.
 */
public class BatchAnalyzeState implements State {
    private BatchSessionContext batchContext;

    public BatchAnalyzeState(BatchSessionContext batchContext) {
        this.batchContext = Objects.requireNonNull(batchContext,
            () -> "BatchContext must be not null, msg: " + toString());
    }

    @Override public boolean handle() throws QuitException {
        batchContext.reset();
        doHandle();
        return false;
    }

    private void doHandle() {
        Com_Query comQuery = Com_Query.loadFromPacket(batchContext.getPacket());
        EleMetaParser parser = EleMetaParser.parse(comQuery.getQuery());
        List<BatchQuery> querys = analyzeSql(parser);
        generateBatchQuerys(comQuery, parser, querys);
        if (!batchContext.getQuerySqls().isEmpty()) {
            batchContext.enqueue(SESSION_STATUS.BATCH_HANDLE);
        }
    }

    private void generateBatchQuerys(Com_Query comQuery, EleMetaParser parser,
        List<BatchQuery> querys) {
        Map<String, BatchQuery> querySqls = new HashMap<String, BatchQuery>();
        querys.forEach((batchQuery) -> {
            batchQuery.setSequenceId(comQuery.sequenceId);
            Map<String, String> eleMeta = new HashMap<>(parser.getEleMeta());
            eleMeta.put(Constants.ELE_META_TRANSID, batchContext.transId);
            setEtraceInfo(eleMeta);
            batchQuery.setEleMeta(eleMeta);
            batchQuery.resetComment(batchContext.getGroupName(), batchContext.isAutoCommit4Client(),
                batchContext.isBindMaster());
            querySqls.put(batchQuery.getShardTable(), batchQuery);
        });
        batchContext.setQuerySqls(querySqls);
    }

    private List<BatchQuery> analyzeSql(EleMetaParser parser) {
        ShardingBatchSQL shardingBatchSQL = ShardingBatchSQL
            .handleBatchSQLBasicly(parser.getQueryWithoutComment(), parser.getQueryComment(),
                batchContext.getShardingRouter());
        shardingBatchSQL.batchTransLogIdStr = batchContext.batchTransLogIdStr;
        ShardingBatchSQL.handleSQLSharding(shardingBatchSQL);
        return shardingBatchSQL.getResultShardingQuerys();
    }

    private void setEtraceInfo(Map<String, String> eleMeta) {
        String rid = batchContext.etraceCurrentRequestId();
        String appid = Constants.APPID;
        String rpcid = batchContext.etraceNextLocalRpcId();

        if (isValidEtraceInfo(rid, rpcid, appid)) {
            eleMeta.put("rid", rid);
            eleMeta.put("appid", appid);
            eleMeta.put("rpcid", rpcid);
        }
    }

    private boolean isValidEtraceInfo(String rid, String rpcid, String appid) {
        return StringUtils.isNotEmpty(rid) && StringUtils.isNotEmpty(rpcid) && StringUtils
            .isNotEmpty(appid);
    }

    @Override public SESSION_STATUS getStatus() {
        return SESSION_STATUS.BATCH_ANALYZE;
    }

    @Override public String toString() {
        return "BatchAnalyzeState";
    }
}
