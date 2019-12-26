package me.ele.jarch.athena.sharding.sql;

import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.scheduler.DBChannelDispatcher;
import me.ele.jarch.athena.sql.EleMetaParser;
import me.ele.jarch.athena.util.KVChainParser;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 用于保存主状态机和batch状态机之间的转换条件
 *
 * @author shaoyang.qi
 */
public class Send2BatchCond {
    private String dalGroupName = "";
    private boolean needBatchAnalyze = false;
    // 保存batch传输的comment，在DAL流程中使用该comment来做batch相关分析，防止远程登录通过comment进行状态注入
    private String batchSendComment = "";
    private Map<String, String> eleMeta = new HashMap<>();
    private boolean isBatchAllowed = false;

    public boolean isBatchAllowed() {
        return isBatchAllowed;
    }

    public Send2BatchCond setDalGroupNameAndInitBatchAllowed(String dalGroupName) {
        if (Objects.isNull(dalGroupName)) {
            return this;
        }

        this.dalGroupName = dalGroupName;
        DBChannelDispatcher holder = DBChannelDispatcher.getHolders().get(dalGroupName);
        isBatchAllowed = holder != null && holder.isBatchAllowed();
        return this;
    }

    /**
     * 此方法仅供shardingSQL子类使用，用于设置needBatchAnalyze，子类可以选择不使用
     *
     * @return
     */
    public boolean resetAndGetNeedBatchAnalyze() {
        if (!isBatchAllowed()) {
            setNeedBatchAnalyze(false);
            return isNeedBatchAnalyze();
        }
        // 所有未经过batch分析的sql，都先进入batch进行分析
        if (!isShuffledByBatchContext()) {
            setNeedBatchAnalyze(true);
        }
        return isNeedBatchAnalyze();
    }

    public boolean isNeedBatchAnalyze() {
        return needBatchAnalyze;
    }

    public Send2BatchCond setNeedBatchAnalyze(boolean needBatchAnalyze) {
        this.needBatchAnalyze = needBatchAnalyze;
        return this;
    }

    public Send2BatchCond setBatchSendComment(String batchSendComment) {
        this.batchSendComment = batchSendComment;
        this.eleMeta.clear();
        if (StringUtils.isNotEmpty(this.batchSendComment)) {
            EleMetaParser.EleCommentIndexer indexer =
                EleMetaParser.searchEleMetaCommentIndexer(batchSendComment);
            this.eleMeta.putAll(KVChainParser.parse(batchSendComment
                .substring(indexer.leftContentIndex + 1, indexer.rightContentIndex)));
        }
        return this;
    }

    public String getBatchSendComment() {
        return batchSendComment;
    }

    public String getValueFromComment(String key) {
        String result = "";
        if (this.eleMeta.isEmpty() || StringUtils.isEmpty(key)) {
            return result;
        }
        return eleMeta.get(key);
    }

    public boolean isShuffledByBatchContext() {
        return this.eleMeta.getOrDefault(Constants.ELE_META_BATCH_ANALYZED_MARKER, null) != null;
    }

    public Send2BatchCond copyValuesFrom(Send2BatchCond sourceCond) {
        this.dalGroupName = sourceCond.dalGroupName;
        this.needBatchAnalyze = sourceCond.needBatchAnalyze;
        this.isBatchAllowed = sourceCond.isBatchAllowed;
        this.setBatchSendComment(sourceCond.batchSendComment);
        return this;
    }

    public static Send2BatchCond getDefault() {
        return FixedSend2BatchCond.DEFAULT;
    }

    private static class FixedSend2BatchCond extends Send2BatchCond {
        private static final Send2BatchCond DEFAULT = new FixedSend2BatchCond();

        @Override public boolean isBatchAllowed() {
            return false;
        }

        @Override public Send2BatchCond setDalGroupNameAndInitBatchAllowed(String dalGroupName) {
            return this;
        }

        @Override public boolean resetAndGetNeedBatchAnalyze() {
            return false;
        }

        @Override public boolean isNeedBatchAnalyze() {
            return false;
        }

        @Override public Send2BatchCond setNeedBatchAnalyze(boolean needBatchAnalyze) {
            return this;
        }

        @Override public Send2BatchCond setBatchSendComment(String batchSendComment) {
            return this;
        }

        @Override public String getBatchSendComment() {
            return super.getBatchSendComment();
        }

        @Override public String getValueFromComment(String key) {
            return super.getValueFromComment(key);
        }

        @Override public boolean isShuffledByBatchContext() {
            return super.isShuffledByBatchContext();
        }

        @Override public Send2BatchCond copyValuesFrom(Send2BatchCond sourceCond) {
            return this;
        }
    }
}
