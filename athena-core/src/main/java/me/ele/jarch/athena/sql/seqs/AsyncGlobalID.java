package me.ele.jarch.athena.sql.seqs;

import com.github.mpjct.jmpjct.util.ErrorCode;
import io.etrace.common.Constants;
import io.netty.buffer.ByteBufUtil;
import me.ele.jarch.athena.allinone.DBConnectionInfo;
import me.ele.jarch.athena.constant.Metrics;
import me.ele.jarch.athena.constant.TraceNames;
import me.ele.jarch.athena.server.async.AsyncClient;
import me.ele.jarch.athena.sql.ResultSet;
import me.ele.jarch.athena.sql.ResultType;
import me.ele.jarch.athena.util.*;
import me.ele.jarch.athena.util.etrace.DalMultiMessageProducer;
import me.ele.jarch.athena.util.etrace.MetricFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

public class AsyncGlobalID extends AsyncClient {
    private static final Logger logger = LoggerFactory.getLogger(AsyncGlobalID.class);
    // 使得获取种子的tps和普通sql的tps的metrics格式相同
    @SuppressWarnings("serial") private static final Map<String, String> SEQS_QUERY_TYPE_TAGS =
        new TreeMap<String, String>() {
            {
                put("sql", "MULTI_TRANS");
            }
        };
    //eg: shipping_sh
    private static final String SEQ_NAME_TEMPLATE = "%s_%s";
    private static final String SELECT_FOR_UPDATE_TEMPLATE =
        "SELECT last_value FROM %s WHERE seq_name = '%s' FOR UPDATE";
    private static final String UPDATE_TEMPLATE =
        "UPDATE %s SET last_value = %s WHERE seq_name = '%s'";
    private final int cache_pool;
    private final SeqsCacheData seqsCacheData;

    private LocalDateTime time_of_birth = LocalDateTime.now();

    private final String seqTable;
    private final String seq_name;

    private final String seq_env;

    private final ConcurrentLinkedQueue<SeqsHandler> ReqQ = new ConcurrentLinkedQueue<>();
    private long last_value = -1;

    private volatile DalMultiMessageProducer producer =
        DalMultiMessageProducer.createEmptyProducer();
    private volatile String curDalGroup = "";
    private StringBuilder errMsg = new StringBuilder();

    private volatile GlobalId globalId;

    private volatile StringBuffer globalTimeoutTracer = new StringBuffer();


    public enum GID_STATUS {
        ok, select_for_update, update, recycle_seed, recycle_commit, commit, quit
    }


    private volatile GID_STATUS status = GID_STATUS.ok;

    public AsyncGlobalID(DBConnectionInfo dbConnInfo, String seq_name,
        SeqsCacheData seqsCacheData) {
        super(dbConnInfo);
        this.seq_env = dbConnInfo.getAdditionalDalCfg()
            .getOrDefault(me.ele.jarch.athena.constant.Constants.DAL_SEQUENCE_ENV,
                EnvConf.get().idc());
        this.seq_name = seq_name;
        cache_pool = AthenaConfig.getInstance().getDalSeqsCachePool();
        this.seqsCacheData = seqsCacheData;
        seqTable = dbConnInfo.getAdditionalDalCfg()
            .getOrDefault(me.ele.jarch.athena.constant.Constants.DAL_SEQUENCE_TABLE,
                AthenaConfig.getInstance().getDalSeqsTable());
        updateGlobalId();
    }

    public GID_STATUS getAsyncGlobalIDStatus() {
        return status;
    }

    public void offer(SeqsHandler handler) {
        handleGlobalID(handler);
    }

    // 从db中获取种子
    private synchronized void refetchFromDB() {
        addTimeoutTracerMsg("try refetchFromDB");
        if (status == GID_STATUS.ok) {
            status = GID_STATUS.select_for_update;
            addTimeoutTracerMsg("trigger refetchFromDB");
            boolean isDo = doAsyncExecute();
            addTimeoutTracerMsg("send_select_for_update_isDo=" + isDo);
        }
    }

    public void updateGlobalId() {
        this.globalId = GlobalId.getGlobalIdBySeqName(seq_name);
    }

    private String activeSeqName() {
        if (!GreySwitch.getInstance().isAllowGlobalIdPartition()) {
            return this.seq_name;
        }
        if (Objects.isNull(globalId)) {
            MetricFactory.newCounter(Metrics.CONFIG_OUTDATED)
                .addTag(TraceNames.DALGROUP, curDalGroup).once();
            return String.format(SEQ_NAME_TEMPLATE, this.seq_name, this.seq_env);
        }
        return globalId.zoneSeqName;
    }

    @Override protected String getQuery() {
        addTimeoutTracerMsg("getQuery() status = " + status);
        if (status == GID_STATUS.select_for_update) {
            producer.startTransaction("SEQ", seqTable + "." + "select_for_update");
            addCommonTagsForCurProducer();
            producer.addPattern2CurTransaction(
                String.format(SELECT_FOR_UPDATE_TEMPLATE, seqTable, activeSeqName()));
            String selectForUpdate =
                String.format(SELECT_FOR_UPDATE_TEMPLATE, seqTable, activeSeqName());
            logger.info("asyncConnId={} send sequence sql [{}] to db [{}]", connectionId,
                selectForUpdate, dbConnInfo.getQualifiedDbId());
            return selectForUpdate;
        }
        if (status == GID_STATUS.update) {
            if (last_value == -1) {
                throw new RuntimeException("[getQuery] invalid last_value=" + last_value);
            }
            producer.startTransaction("SEQ", seqTable + "." + "update");
            addCommonTagsForCurProducer();
            producer.addPattern2CurTransaction(
                String.format(UPDATE_TEMPLATE, seqTable, "?", activeSeqName()));
            String update = String
                .format(UPDATE_TEMPLATE, seqTable, String.valueOf((last_value + cache_pool)),
                    activeSeqName());
            logger.info("asyncConnId={} send sequence sql [{}] to db [{}]", connectionId, update,
                dbConnInfo.getQualifiedDbId());
            return update;
        }
        if (status == GID_STATUS.recycle_seed) {
            producer.startTransaction("SEQ", seqTable + "." + "recycle");
            addCommonTagsForCurProducer();
            producer.addPattern2CurTransaction(
                String.format(UPDATE_TEMPLATE, seqTable, "?", activeSeqName()));
            String update = String
                .format(UPDATE_TEMPLATE, seqTable, String.valueOf(globalId.minSeed),
                    activeSeqName());
            logger.info("asyncConnId={} send sequence sql [{}] to db [{}]", connectionId, update,
                dbConnInfo.getQualifiedDbId());
            return update;
        }
        if (status == GID_STATUS.commit || status == GID_STATUS.recycle_commit) {
            producer.startTransaction("SEQ", seqTable + "." + status.name());
            addCommonTagsForCurProducer();
            producer.addPattern2CurTransaction("COMMIT");
            if (status == GID_STATUS.recycle_commit) {
                MetricFactory.newCounter("globalId.recycle")
                    .addTag(TraceNames.DALGROUP, curDalGroup).addTag(TraceNames.SEED, seq_name)
                    .addTag(TraceNames.TABLE, seqTable).once();
            }
            logger.info("asyncConnId={} send sequence sql [{}] to db [{}]", connectionId, "COMMIT",
                dbConnInfo.getQualifiedDbId());
            return "COMMIT";
        }
        logger.error("[getQuery] other status=" + status);
        throw new RuntimeException("[getQuery] other status=" + status);
    }

    private synchronized void handleGlobalID(SeqsHandler handler) {
        if (status == GID_STATUS.quit) {
            // 防止连接断开并调用过doQuit后状态为quit时,客户端仍然走到此方法内导致的放入ReqQ后不kill产生的阻塞
            handleGlobalID(handler, -1, false);
            return;
        }
        if (handler != null) {
            ReqQ.offer(handler);
        }
        if (status != GID_STATUS.ok) {
            // 如果当前状态不处于ok,也就是处于连接db或select for update获取种子期间,直接返回
            return;
        }
        while (!ReqQ.isEmpty()) {
            List<Long> nextids = this.seqsCacheData.getNextSeqValue(ReqQ.peek().getCount());
            if (nextids != null && isCacheValid()) {
                handleGlobalID(ReqQ.poll(), nextids, true);
            } else {
                this.producer = ReqQ.peek().getProducer();
                this.curDalGroup = ReqQ.peek().getGroup();
                refetchFromDB();
                return;
            }
        }
    }

    private boolean isCacheValid() {
        if (globalId == null || !globalId.recycled) {
            return true;
        }
        Duration duration = Duration.between(time_of_birth, LocalDateTime.now());
        if (duration.getSeconds() > globalId.recycled_cache_lifecycle) {
            MetricFactory.newCounter("globalId.expire.recycle")
                .addTag(TraceNames.DALGROUP, curDalGroup).addTag(TraceNames.SEED, seq_name)
                .addTag(TraceNames.TABLE, seqTable).once();
            return false;
        }
        return true;
    }

    private void handleGlobalID(SeqsHandler handler, long globalID, boolean success) {
        if (handler != null) {
            List<String> list = new ArrayList<>();
            try {
                list.add(String.valueOf(handler.getCalc().calc(globalID, handler.getParams())));
                handler.clientWriteGlobalID(
                    GeneratorUtil.getIDPackets("next_value", list).toArray(new byte[0][]), success,
                    errMsg.toString());
            } catch (Exception e) {
                logger.error("", e);
                handler.clientWriteGlobalID(
                    GeneratorUtil.getIDPackets("next_value", list).toArray(new byte[0][]), false,
                    String.format("some error happened due to %s", e.getMessage()));
            }
        }
    }

    private void handleGlobalID0(SeqsHandler handler, List<Long> range, boolean success) {
        List<String> list = new ArrayList<>();
        try {
            if (handler.isRange() && handler.isSerial()) {
                handleSerialRange(range, list, handler, success);
            } else if (handler.isRange() && !handler.isSerial()) {
                handleNotSerialRange(range, list, handler, success);
            } else {
                long start = System.currentTimeMillis();
                for (long seqId : range) {
                    list.add(String.valueOf(handler.getCalc().calc(seqId, handler.getParams())));
                }
                handler.getProducer()
                    .addTag("calculateDur", String.valueOf((System.currentTimeMillis() - start)));
                handler.clientWriteGlobalID(
                    GeneratorUtil.getIDPackets("next_value", list).toArray(new byte[0][]), success,
                    errMsg.toString());
            }
        } catch (Exception e) {
            logger.error("", e);
            handler.clientWriteGlobalID(
                GeneratorUtil.getIDPackets("next_value", list).toArray(new byte[0][]), false,
                String.format("some error happened due to %s", e.getMessage()));
        }
    }

    private void handleGlobalID(SeqsHandler handler, List<Long> range, boolean success) {
        if (handler != null) {
            handleGlobalID0(handler, range, success);
        }
    }

    private void handleSerialRange(List<Long> range, List<String> list, SeqsHandler handler,
        boolean success) throws SeqsException {
        long start = System.currentTimeMillis();
        if (range.size() == 1) {
            //允许range的begin和end 相等
            range.add(range.get(0));
        }
        if (range.size() != 2) {
            throw new SeqsException("there should be only one start and one end: " + range
                + ",please check globalid sql");
        }
        for (long seqId : range) {
            list.add(String.valueOf(handler.getCalc().calc(seqId, handler.getParams())));
        }
        handler.getProducer()
            .addTag("calculateDur", String.valueOf((System.currentTimeMillis() - start)));
        handler.clientWriteGlobalID(
            GeneratorUtil.getSerialIDPackets("next_begin", "next_end", list).toArray(new byte[0][]),
            success, errMsg.toString());
    }

    private void handleNotSerialRange(List<Long> range, List<String> list, SeqsHandler handler,
        boolean success) throws SeqsException {
        long startTime = System.currentTimeMillis();
        if (range.size() == 1) {
            list.add(String.valueOf(handler.getCalc().calc(range.get(0), handler.getParams())));
        } else if (range.size() == 2) {
            long start = range.get(0);
            long end = range.get(1);
            for (; start <= end; start++) {
                list.add(String.valueOf(handler.getCalc().calc(start, handler.getParams())));
            }
        } else {
            throw new SeqsException("there should be not more than two elements: " + range
                + ",please check globalid sql");
        }
        handler.getProducer()
            .addTag("calculateDur", String.valueOf((System.currentTimeMillis() - startTime)));
        handler.clientWriteGlobalID(
            GeneratorUtil.getIDPackets("next_value", list).toArray(new byte[0][]), success,
            errMsg.toString());
    }

    @Override protected void handleResultSet(ResultSet rs) {
        if (status == GID_STATUS.select_for_update) {
            checkRS(rs);
            if (!rs.next()) {
                throw new RuntimeException(
                    "no rows affected for [SELECT last_value FROM " + seqTable + " WHERE seq_name='"
                        + activeSeqName() + "' FOR UPDATE]");
            }
            last_value = Long.valueOf(rs.getString(1));
            if (checkRecycleDemand()) {
                status = GID_STATUS.recycle_seed;
            } else {
                status = GID_STATUS.update;
            }
            this.producer.setTransactionStatus(Constants.SUCCESS);
            this.producer.completeTransaction();
            addTimeoutTracerMsg("handle select_for_update result");
            boolean isDo = doAsyncExecute();
            addTimeoutTracerMsg("send_update_isDo=" + isDo);
            return;
        }
        if (status == GID_STATUS.update) {
            checkOK(rs);
            status = GID_STATUS.commit;
            this.producer.setTransactionStatus(Constants.SUCCESS);
            this.producer.completeTransaction();
            addTimeoutTracerMsg("handle update result");
            boolean isDo = doAsyncExecute();
            addTimeoutTracerMsg("send_commit_isDo=" + isDo);
            return;
        }
        if (status == GID_STATUS.recycle_seed) {
            checkOK(rs);
            status = GID_STATUS.recycle_commit;
            this.producer.setTransactionStatus(Constants.SUCCESS);
            this.producer.completeTransaction();
            addTimeoutTracerMsg("handle recycle result");
            boolean isDo = doAsyncExecute();
            addTimeoutTracerMsg("send_recycle_commit_isDo=" + isDo);
            return;
        }
        if (status == GID_STATUS.recycle_commit) {
            checkOK(rs);
            status = GID_STATUS.ok;
            refetchFromDB();
            return;
        }
        if (status == GID_STATUS.commit) {
            checkOK(rs);
            long seq_id_Base = last_value;
            seqsCacheData.appendSeqSeed(last_value, this.cache_pool, getDbConnInfo());
            last_value = -1;
            status = GID_STATUS.ok;
            this.producer.setTransactionStatus(Constants.SUCCESS);
            this.producer.completeTransaction();
            this.producer = DalMultiMessageProducer.createEmptyProducer();
            logger.info("fetch new globalid seed succeed,seq_id_Base = " + seq_id_Base);
            MetricFactory.newCounter(Metrics.TRANS_GLOBALID_TPS)
                .addTag(TraceNames.DALGROUP, curDalGroup)
                .addTag(TraceNames.DBID, getDbConnInfo().getQualifiedDbId()).once();
            checkCurDalGroupAccessRight();
            checkSeedUsageRate(seq_id_Base);
            globalTimeoutTracer = new StringBuffer();
            time_of_birth = LocalDateTime.now();
            handleGlobalID(null);
            return;
        }
        logger.error("[handleResultSet] other status:" + status);
        throw new RuntimeException("[handleResultSet] other status:" + status);
    }

    private void checkOK(ResultSet resultSet) {
        ResultType type = resultSet.getResultType();
        String errMsg = "";
        if (resultSet.getErr() != null) {
            errMsg = ",errMsg=" + resultSet.getErr().errorMessage;
        }
        if (type != ResultType.OK) {
            throw new RuntimeException(
                "response type is not OK when status = " + status + ",type=" + type + errMsg);
        }
    }

    private void checkRS(ResultSet resultSet) {
        ResultType type = resultSet.getResultType();
        String errMsg = "";
        if (resultSet.getErr() != null) {
            errMsg = ",errMsg=" + resultSet.getErr().errorMessage;
        }
        if (type != ResultType.RESULT_SET) {
            throw new RuntimeException(
                "response type is not RESULT_SET when status = " + status + ",type=" + type
                    + errMsg);
        }
    }

    private void checkCurDalGroupAccessRight() {
        if (Objects.isNull(globalId)) {
            logger.error("no related config found in dal-globalids.yml for seed {}", seq_name);
            return;
        }
        if (!globalId.attachedDALGroups.contains(curDalGroup)) {
            logger.error("Trying to fetch global ID for illegal dalgroup {},  seq name {}",
                curDalGroup, seq_name);
            MetricFactory.newCounter(Metrics.GLOBALID_DISALLOWED_DALGROUP)
                .addTag(TraceNames.DALGROUP, curDalGroup).once();
        }
    }

    private void checkSeedUsageRate(long seed) {
        MetricFactory.newGauge(Metrics.GLOBALID_SEED_VALUE).addTag(TraceNames.SEED, seq_name)
            .value(seed);
        if (Objects.isNull(globalId)) {
            MetricFactory.newCounter(Metrics.GLOBALID_CONFIG_EMPTY)
                .addTag(TraceNames.SEED, seq_name).once();
            logger.debug(
                "no related config found in dal-globalids.yml for seed: {}, skip check seed usage rate",
                seq_name);
            return;
        }
        if (globalId.recycled) {
            return;
        }
        if (globalId.seedCount() <= 0) {
            logger.error("Illegal seed range[{}, {}] for seq_name:{} in zone: {}", globalId.minSeed,
                globalId.maxSeed, seq_name, globalId.zoneName);
            MetricFactory.newCounter(Metrics.GLOBALID_CONFIG_ERROR)
                .addTag(TraceNames.SEED, seq_name).once();
            return;
        }
        double seedUsageRate = (100 * (seed - globalId.minSeed)) / (double) globalId.seedCount();
        logger.debug("seed - min = {}, max - min = {}, util = {}", seed - globalId.minSeed,
            globalId.seedCount(), seedUsageRate);
        MetricFactory.newGauge(Metrics.GLOBALID_SEED_UTIL).addTag(TraceNames.SEED, seq_name)
            .value(seedUsageRate);
    }

    @Override protected void handleTimeout() {
        NoThrow.call(() -> printIssueInfo());
        doQuit("[AsyncGlobalID] handleTimeout");
    }

    private void printIssueInfo() {
        String packetInfo = "";
        if (Objects.nonNull(serverPackets)) {
            packetInfo = this.serverPackets.stream().map(x -> ByteBufUtil.hexDump(x))
                .collect(Collectors.joining(" , "));
        }
        logger.error("connectionId:{} status:{}/{} packet:{},traceInfo:{}", this.connectionId,
            this.status, this.getStatus(), packetInfo, globalTimeoutTracer.toString());
        globalTimeoutTracer = new StringBuffer();
    }

    @Override protected void handleChannelInactive() {
        doQuit("[AsyncGlobalID] handleChannelInactive");
    }

    private synchronized void close() {
        status = GID_STATUS.quit;
        SeqsHandler handler = null;
        while ((handler = ReqQ.poll()) != null) {
            handleGlobalID(handler, -1, false);
        }
    }

    @Override public synchronized void doQuit(String reason) {
        if (!"".equals(reason)) {
            errMsg.append(" ").append(reason);
        }
        String status =
            String.format("%s:%s", ResponseStatus.ResponseType.ABORT, ErrorCode.ABORT_GLOBAL_ID);
        this.producer.addTag("errorMsg", errMsg.toString());
        this.producer.setTransactionStatus(status);
        this.producer.completeTransaction();
        this.producer = DalMultiMessageProducer.createEmptyProducer();
        close();
        super.doQuit(errMsg.toString());
    }

    private void addCommonTagsForCurProducer() {
        producer.addTag("dbId", getDbConnInfo().getQualifiedDbId());
        producer.addTag("connId", String.valueOf(connectionId));
    }

    private void addTimeoutTracerMsg(String msg) {
        globalTimeoutTracer.append("<").append(LocalTime.now()).append(" ")
            .append(Thread.currentThread()).append(" ").append(msg).append(">");
    }

    @Override protected void execute() {
        addTimeoutTracerMsg("execute() status=" + getStatus() + " globalid_status=" + status);
        super.execute();
    }

    private boolean checkRecycleDemand() {
        if (globalId == null || !globalId.recycled) {
            return false;
        }
        if ((last_value + cache_pool) > globalId.maxSeed) {
            return true;
        }
        return false;
    }
}
