package me.ele.jarch.athena.util.rmq;

import me.ele.jarch.athena.detector.DetectorDelegate;
import me.ele.jarch.athena.netty.AthenaServer;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.util.JacksonObjectMappers;
import me.ele.jarch.athena.util.NoThrow;
import me.ele.jarch.athena.util.SafeJSONHelper;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * decide whether send sql to arch.q or not.
 *
 * @author shaoyang.qi
 */
public class AuditSqlToRmq {
    // sql pattern缓存数量
    private static final int SQL_ID_SIZE = 10000;
    // 缓冲的sqlInfo数量
    private static final int BUFFER_SIZE = 50;
    // 每10s执行一次发送
    private static final long FLUSH_INTERVAL_MILLI = 200;
    // sqlInfo sender
    private final RmqSender sender = new RmqSender();
    // <sqlPattern>
    private final Set<String> sqlIdSet = Collections.newSetFromMap(new ConcurrentHashMap<>());
    // buffer the RmqSqlInfos needed to be sent
    private final Queue<RmqSqlInfo> sqlInfoList = new ConcurrentLinkedQueue<>();
    // current buffer size of the RmqSqlInfos needed to be sent
    private final AtomicInteger currentBufferSize = new AtomicInteger(0);

    private AuditSqlToRmq() {
        AthenaServer.rmqJobScheduler
            .addJob("sendSqlToRmq", FLUSH_INTERVAL_MILLI, this::sendSqlInfos);
        AthenaServer.rmqJobScheduler
            .addJob("sendSlowSqlToRmq", FLUSH_INTERVAL_MILLI, this::sendSlowSqlInfo);
    }

    private static class INNER {
        private static final AuditSqlToRmq INSTANCE = new AuditSqlToRmq();
    }

    public static AuditSqlToRmq getInstance() {
        return INNER.INSTANCE;
    }

    public void reset() {
        sqlInfoList.clear();
        currentBufferSize.getAndSet(0);
        sqlIdSet.clear();
    }

    public void resetChannelOfRmqSender() {
        sender.resetChannel();
    }

    // 首次出现的sql必定发送
    boolean isFirstSend(String sqlId) {
        if (StringUtils.isEmpty(sqlId)) {
            return false;
        }
        if (sqlIdSet.contains(sqlId)) {
            return false;
        }
        if (sqlIdSet.size() >= SQL_ID_SIZE) {
            sqlIdSet.clear();
        }
        sqlIdSet.add(sqlId);
        return true;
    }

    public void trySend(SqlSessionContext ctx) {
        if (Objects.isNull(ctx.rmqSqlInfo)) {
            return;
        }
        RmqSqlInfo info = ctx.rmqSqlInfo;
        ctx.rmqSqlInfo = null;
        if (currentBufferSize.get() > BUFFER_SIZE) {
            return;
        }
        sqlInfoList.add(info);
        currentBufferSize.incrementAndGet();
    }

    private void sendSqlInfos() {
        int currSize = currentBufferSize.get();
        for (int i = 0; i < currSize; i++) {
            RmqSqlInfo sqlInfo = sqlInfoList.poll();
            if (sqlInfo == null) {
                break;
            }
            currentBufferSize.decrementAndGet();
            sendSqlInfo(sqlInfo);
        }
    }

    private void sendSqlInfo(RmqSqlInfo sqlInfo) {
        NoThrow.call(() -> {
            String infoStr = SafeJSONHelper.of(JacksonObjectMappers.getMapper())
                .writeValueAsStringOrDefault(sqlInfo, "");
            if (StringUtils.isEmpty(infoStr)) {
                return;
            }
            byte[] bytes = infoStr.getBytes("UTF-8");
            sender.sendMessage(bytes, sender.normalRoutingKey);
        });
    }

    private void sendSlowSqlInfo() {
        NoThrow.call(() -> {
            List<SlowSqlInfo> infos = new ArrayList<>();
            SlowSqlInfo info = DetectorDelegate.getSlowSqlInfo();
            while (Objects.nonNull(info)) {
                infos.add(info);
                info = DetectorDelegate.getSlowSqlInfo();
            }
            if (infos.size() == 0) {
                return;
            }
            for (SlowSqlInfo slowSqlInfo : infos) {
                String infoStr = SafeJSONHelper.of(JacksonObjectMappers.getMapper())
                    .writeValueAsStringOrDefault(slowSqlInfo, "");
                if (StringUtils.isEmpty(infoStr)) {
                    continue;
                }
                sender.sendMessage(infoStr.getBytes("UTF-8"), sender.slowRoutingKey);
            }
        });
    }
}
