package me.ele.jarch.athena.util.rmq;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by rui.wang07 on 2017/9/30.
 * 发往eagle的SQL样本概率设置
 */
public enum SendThreshold {
    THRES_0("THRES_0", 0, 1000), THRES_10("THRES_10", 10, 100), THRES_100("THRES_100", 100,
        10), THRES_1000("THRES_1000", 1000, 1);

    private static final Logger logger = LoggerFactory.getLogger(SendThreshold.class);
    private final String name;
    // SQL执行时间分级, ms
    private final int sendTimeRank;
    // 抽样比率的分母, 分子为1
    public volatile int threshold;

    SendThreshold(String name, int rank, int threshold) {
        this.name = name;
        this.sendTimeRank = rank;
        this.threshold = threshold;
    }

    /**
     * 根据SQL执行时间不同, 进行分级抽样,
     * [0, 10ms)默认1/1000
     * [10ms, 100ms)默认1/100
     * [100ms, 1s)默认1/10
     * [1s, ∞)默认1/1
     *
     * @param thresholdStr 字符串格式示例:  THRES_0:1000;THRES_10:100;THRES_100:10;THRES_1000:1
     */
    public static void parseThresholds(String thresholdStr) {
        if (StringUtils.isEmpty(thresholdStr)) {
            return;
        }
        logger.info("Changing rmq sql send threshold: {}", thresholdStr);
        if (thresholdStr.endsWith(";")) {
            thresholdStr = thresholdStr.substring(0, thresholdStr.length() - 1);
        }
        String[] parts = thresholdStr.split(";");
        if (parts.length != 4) {
            return;
        }
        EnumMap<SendThreshold, Integer> newThres = new EnumMap<>(SendThreshold.class);
        Map<String, SendThreshold> thresholdMap = new HashMap<>();
        for (SendThreshold st : SendThreshold.values()) {
            thresholdMap.put(st.name, st);
        }
        try {
            for (String part : parts) {
                String[] kv = part.split(":");
                if (kv.length != 2) {
                    return;
                }
                String name = kv[0].trim().toUpperCase();
                int newThreshold = Integer.parseInt(kv[1].trim());
                if (!thresholdMap.containsKey(name)) {
                    return;
                }
                newThres.put(thresholdMap.get(name), newThreshold);
            }
            if (newThres.size() != 4) {
                return;
            }
            newThres.forEach((k, v) -> {
                k.threshold = v;
            });
            logger.info("Rmq sql send threshold changed: {}", thresholdStr);
        } catch (NumberFormatException e) {
            logger.error("Cannot parse threshold: {}", thresholdStr, e);
        }
    }

    /**
     * 根据传入的sql执行时间，返回对应的阈值
     *
     * @param duration sql执行时间, ms
     * @return 样本总量阈值
     */
    public static int getThreshold(long duration) {
        int threshold = THRES_1000.threshold;
        if (duration < THRES_10.sendTimeRank) {
            threshold = THRES_0.threshold;
        }
        if (duration >= THRES_10.sendTimeRank && duration < THRES_100.sendTimeRank) {
            threshold = THRES_10.threshold;
        }
        if (duration >= THRES_100.sendTimeRank && duration < THRES_1000.sendTimeRank) {
            threshold = THRES_100.threshold;
        }
        return threshold;
    }
}
