package me.ele.jarch.athena.sql.seqs.generator;

import me.ele.jarch.athena.sql.seqs.GeneratorUtil;
import me.ele.jarch.athena.sql.seqs.SeqsException;
import me.ele.jarch.athena.sql.seqs.SeqsHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * +------------+----------------------+
 * | 最多27位字符 |    5位16进制字符      |
 * +------------+----------+-----------+
 * |   prefix   | firstDim | secondDim |
 * +------------+----------+-----------+
 * Created by jinghao.wang on 2017/10/31.
 */
public class TwoDimTaskGenerator extends Generator {
    private static final Logger LOG = LoggerFactory.getLogger(TwoDimTaskGenerator.class);
    // 在taskId字段里末尾有5位16进制字符是嵌入的hash信息
    private static final int HASH_LENGTH = 5;

    private final String prefix;
    private final String firstDim;
    private final String secondDim;

    public TwoDimTaskGenerator(String prefix, String firstDim, String secondDim) {
        this.prefix = prefix;
        this.firstDim = firstDim;
        this.secondDim = secondDim;
    }

    @Override public void getGlobalID(SeqsHandler handler) throws Exception {
        checkParams(handler.getParams());
        List<String> list = new ArrayList<>();
        list.add(calc(0, handler.getParams()));
        handler.clientWriteGlobalID(
            GeneratorUtil.getIDPackets("next_value", list).toArray(new byte[0][]), true, "");
    }

    protected void checkParams(Map<String, String> params) throws SeqsException {
        Map<String, String> kv = new HashMap<>(params);
        kv.remove(GeneratorUtil.BIZ);
        kv.remove(GeneratorUtil.SEQ_NAME);
        kv.remove(GeneratorUtil.COUNT);
        if (!kv.containsKey(prefix)) {
            throw new SeqsException("missing " + prefix + " info" + ERR_MSG);
        }
        if (!kv.containsKey(firstDim)) {
            throw new SeqsException("missing " + firstDim + " info" + ERR_MSG);
        }
        if (!kv.containsKey(secondDim)) {
            throw new SeqsException("missing " + secondDim + " info" + ERR_MSG);
        }
        kv.remove(prefix);
        kv.remove(firstDim);
        kv.remove(secondDim);
        if (!kv.isEmpty()) {
            throw new SeqsException("too many useless params: " + kv + ERR_MSG);
        }
    }

    /**
     * @param globalID 由于此generator的前缀填充值由Client的SQL输入,所以此处作为种子传入的值实际上并没有使用,可以是任意值
     * @param params
     * @return
     * @throws SeqsException
     */
    @Override public String calc(long globalID, Map<String, String> params) throws SeqsException {
        //check ID
        String prefixValue = params.get(prefix);
        if (Objects.isNull(prefixValue))
            throw new SeqsException(prefix);
        String taskId;
        if (prefixValue.length() < 27)
            taskId = prefixValue.substring(0, prefixValue.length());
        else
            taskId = prefixValue.substring(0, 27);

        String firstDimValue = params.get(firstDim);
        if (Objects.isNull(firstDimValue)) {
            throw new SeqsException(firstDim);
        }
        String secondDimValue = params.get(secondDim);
        if (Objects.isNull(secondDimValue)) {
            throw new SeqsException(secondDim);
        }
        long firstDimHash = getHashCode(firstDimValue);
        long secondDimHash = getHashCode(secondDimValue);
        long hashNum = (firstDimHash << 10) | secondDimHash;
        String fixedHex = Long.toHexString(hashNum);
        while (fixedHex.length() < HASH_LENGTH) {
            fixedHex = "0" + fixedHex;
        }
        taskId = taskId + fixedHex;
        return taskId.toLowerCase();
    }

    @Override public long decode(String column, String globalID) {
        // 后20位的信息(包括第一维和第二维的hash信息)
        Long hashNum;
        try {
            hashNum = Long.parseLong(
                globalID.substring(globalID.length() - HASH_LENGTH, globalID.length()), 16);
        } catch (Exception ignore) {
            LOG.error("Parse globalID failed", ignore);
            return -1;
        }

        long result = -1;
        if (column.equals(firstDim))
            result = hashNum >> 10;
        if (column.equals(secondDim))
            result = hashNum - (hashNum >> 10 << 10);
        return result;
    }
}
