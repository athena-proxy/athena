package me.ele.jarch.athena.sql.seqs.generator;

import me.ele.jarch.athena.sql.seqs.GeneratorUtil;
import me.ele.jarch.athena.sql.seqs.SeqsCache;
import me.ele.jarch.athena.sql.seqs.SeqsException;
import me.ele.jarch.athena.sql.seqs.SeqsHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class ComposedGenerator extends CommonGenerator {
    public static final Logger logger = LoggerFactory.getLogger(ComposedGenerator.class);

    @Override public long decode(String column, String globalID) {
        Long orderid;
        try {
            orderid = Long.parseLong(globalID);
        } catch (Exception ignore) {
            logger.error("Parse globalID failed", ignore);
            return -1;
        }
        return orderid & 1023;
    }

    public static long getHashCodeValue(Collection<String> list) {
        long hash = 1;
        for (String id : list) {
            if (checkNumID(id)) {
                hash = Math.abs(hash * Long.parseLong(id));
            } else {
                // getHashCodeValue方法原先的实现为:
                // 当传入N个数字a,b,c..时,返回getHashCode(a*b*c*..)
                // 当传入N个非数字a,b,c...时,返回getHashCode(getHashCode(a)*getHashCode(b))
                // 这样就导致一个问题:如果只传入一个非数字时,getHashCode(getHashCode(a))生成的hash返回只能是getHashCode(0-1023),结果只有437个值
                // 所以把它的行为修改为:如果传入list仅有一个值且为非数字型,那么直接返回其hashcode
                // 这样,这个API在传入一个参数时(无论是否是数字型),都能够和Generator.getHashCode(String)保持行为一致
                if (list.size() == 1) {
                    return getHashCode(id);
                }
                hash = Math.abs(hash * getHashCode(id));
            }
        }
        return getHashCode(String.valueOf(hash));
    }

    private static long getHashCodeValue(Map<String, String> params) {
        Map<String, String> kv = new HashMap<>(params);
        kv.remove(GeneratorUtil.BIZ);
        kv.remove(GeneratorUtil.COUNT);
        kv.remove(GeneratorUtil.SEQ_NAME);
        return getHashCodeValue(kv.values());
    }

    protected void checkParams(Map<String, String> params) throws SeqsException {
        Map<String, String> kv = new HashMap<>(params);
        kv.remove(GeneratorUtil.COUNT);
        if (kv.size() == 2) {// biz, seq_name
            throw new SeqsException("missing more key info" + ERR_MSG);
        }
    }

    @Override public void getGlobalID(SeqsHandler handler) throws Exception {
        String biz_seq_name = getParam(handler.getParams(), GeneratorUtil.BIZ);
        checkParams(handler.getParams());
        SeqsCache seqsCache = getSeqsCache(biz_seq_name, handler.getGroup());
        seqsCache.getSeqValue(handler);
    }

    @Override public String calc(long globalID, Map<String, String> params) throws SeqsException {
        return String.valueOf((globalID << 10) + getHashCodeValue(params));
    }
}
