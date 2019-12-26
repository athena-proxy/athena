package me.ele.jarch.athena.util.etrace;

import io.etrace.agent.message.callstack.MultiCallstackProducer;
import io.etrace.common.modal.Transaction;
import me.ele.jarch.athena.netty.AthenaServer;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public abstract class DalMultiMessageProducer {

    public static DalMultiMessageProducer createProducer() {
        if ((ThreadLocalRandom.current().nextInt(100) + 1) <= AthenaServer.globalZKCache
            .getEtraceSampleRate()) {
            return new RealDalMultiMessageProducer(MultiCallstackProducer.createProducer());
        } else {
            return createEmptyProducer();
        }
    }

    public static DalMultiMessageProducer createEmptyProducer() {
        return NOOP;
    }

    public abstract void startTransaction(String type, String name);

    public abstract Transaction startTransactionAndGet(String type, String name);

    public abstract void addTag(String key, String value);

    public abstract void setTransactionStatus(Throwable throwable);

    public abstract void setTransactionStatus(String status);

    public abstract void completeTransaction();

    public abstract void completeAllTransaction();

    public abstract void continueTrace(String requestId, String rpcId);

    /**
     * 该方法传入的pattern只会让etrace发送一次,即重复调用无效
     *
     * @param pattern
     */
    public abstract void addPattern2CurTransaction(String pattern);

    /**
     * 该方法传入的pattern只会让etrace发送一次,即重复调用无效
     * 在sqlhash外部已提前生成的情况下，调用此方法能够减少hash计算
     *
     * @param pattern
     * @param sqlHash
     */
    public abstract void addPattern2CurTransaction(String pattern, String sqlHash);

    public abstract void clean();

    public abstract boolean isEmpty();

    public abstract String nextLocalRpcId();

    public abstract String nextRemoteRpcId();

    public abstract String currentRequestId();

    public abstract FluentEvent newFluentEvent(String type, String name);

    public abstract Map<String, String> getAllDalTags();

    static final DalMultiMessageProducer NOOP = new DalMultiMessageProducer() {
        @Override public void startTransaction(String type, String name) {

        }

        @Override public Transaction startTransactionAndGet(String type, String name) {
            return null;
        }

        @Override public void addTag(String key, String value) {

        }

        @Override public void setTransactionStatus(Throwable throwable) {

        }

        @Override public void setTransactionStatus(String status) {

        }

        @Override public void completeTransaction() {

        }

        @Override public void completeAllTransaction() {

        }

        @Override public void continueTrace(String requestId, String rpcId) {

        }

        @Override public void addPattern2CurTransaction(String pattern) {

        }

        @Override public void addPattern2CurTransaction(String pattern, String sqlHash) {

        }

        @Override public void clean() {

        }

        @Override public boolean isEmpty() {
            return true;
        }

        @Override public String nextLocalRpcId() {
            return "";
        }

        @Override public String nextRemoteRpcId() {
            return "";
        }

        @Override public String currentRequestId() {
            return "";
        }

        @Override public FluentEvent newFluentEvent(String type, String name) {
            return FluentEvent.NOOP;
        }

        @Override public Map<String, String> getAllDalTags() {
            return Collections.emptyMap();
        }
    };
}
