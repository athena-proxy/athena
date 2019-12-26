package me.ele.jarch.athena.util.etrace;

import io.etrace.agent.message.callstack.MultiCallstackProducer;
import io.etrace.common.Constants;
import io.etrace.common.modal.Transaction;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jinghao.wang on 17/6/27.
 */
class RealDalMultiMessageProducer extends DalMultiMessageProducer {
    private final MultiCallstackProducer producer;
    private volatile String currentType = "";
    private final Map<String, String> dalTags = new HashMap<>();

    RealDalMultiMessageProducer(MultiCallstackProducer producer) {
        this.producer = producer;
    }

    @Override public void startTransaction(String type, String name) {
        this.currentType = type;
        producer.startTransaction(type, name);
    }

    @Override public Transaction startTransactionAndGet(String type, String name) {
        this.currentType = type;
        return producer.startTransactionAndGet(type, name);
    }

    @Override public void addTag(String key, String value) {
        producer.addTag(key, value);
        if ("DAL".equals(currentType)) {
            dalTags.put(key, value);
        }
    }

    @Override public void setTransactionStatus(Throwable throwable) {
        producer.setTransactionStatus(throwable);
    }

    @Override public void setTransactionStatus(String status) {
        producer.setTransactionStatus(status);
    }

    @Override public void completeTransaction() {
        producer.completeTransaction();
    }

    @Override public void completeAllTransaction() {
        producer.completeAllTransaction();
    }

    @Override public void continueTrace(String requestId, String rpcId) {
        producer.continueTrace(requestId, rpcId);
    }

    @Override public void addPattern2CurTransaction(String pattern) {
        if (StringUtils.isEmpty(pattern)) {
            return;
        }
        EtracePatternUtil.SQLRecord sqlRecord = EtracePatternUtil.addAndGet(pattern);
        producer.addTag("sqlId", sqlRecord.hash);
        if (!sqlRecord.isSent2Trace()) {
            producer.addTag("pattern", pattern);
            sqlRecord.markAsSent2Trace();
        }
    }

    @Override public void addPattern2CurTransaction(String pattern, String sqlHash) {
        if (StringUtils.isEmpty(pattern)) {
            return;
        }
        EtracePatternUtil.SQLRecord sqlRecord = EtracePatternUtil.addAndGet(pattern, () -> sqlHash);
        producer.addTag("sqlId", sqlRecord.hash);
        if (!sqlRecord.isSent2Trace()) {
            producer.addTag("pattern", pattern);
            sqlRecord.markAsSent2Trace();
        }
    }

    @Override public void clean() {
        producer.clean();
    }

    @Override public boolean isEmpty() {
        return false;
    }

    @Override public String nextLocalRpcId() {
        String result = producer.nextLocalRpcId();
        producer.logEvent(Constants.TYPE_ETRACE_LINK, Constants.NAME_ASYNC_CALL, Constants.SUCCESS,
            result, null);
        return result;
    }

    @Override public String nextRemoteRpcId() {
        return producer.nextRemoteRpcId();
    }

    @Override public String currentRequestId() {
        return producer.getCurrentRequestId();
    }

    @Override public FluentEvent newFluentEvent(String type, String name) {
        return new FluentEvent(producer.newEvent(type, name));
    }

    @Override public Map<String, String> getAllDalTags() {
        return dalTags;
    }

}
