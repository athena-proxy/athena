package me.ele.jarch.athena.sql.seqs.generator;

import me.ele.jarch.athena.sql.seqs.SeqsCache;
import me.ele.jarch.athena.sql.seqs.SeqsCalc;
import me.ele.jarch.athena.sql.seqs.SeqsException;
import me.ele.jarch.athena.sql.seqs.SeqsHandler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class Generator implements SeqsCalc {
    // 每一个需要去数据库拿种子的dalgroup都有双主自动切换功能
    // 所以每个<biz_seq_name|dalgroup>组合都需要对应SeqsCache
    // 而非一个Generator对应一个SeqsCache
    private final ConcurrentHashMap<String, SeqsCache> seqsCaches = new ConcurrentHashMap<>();

    protected SeqsCache getSeqsCache(String biz_seq_name, String group) {
        return seqsCaches
            .computeIfAbsent(biz_seq_name + "|" + group, (k) -> new SeqsCache(biz_seq_name, group));
    }

    // 类关系:
    // 一个SeqsHandler实例包含一次调用获取globalid所需要的所有信息
    // 其中也保存一个Generator的引用,每种类型的Generator只会有一份实例
    // Generator调用getGlobalID后传入SeqsHandler,用于计算mysql返回last_value转换为globalid(在AsyncGlobalID中)
    // 一个Generator会有多个SeqsCache,每个SeqsCache后端一对一对应dal_sequences表的每一行
    // 单个SeqsCache对应一个AsyncGlobalID实例,在网络中断或异常情况下,会重新new AsyncGlobalID();
    public abstract void getGlobalID(SeqsHandler handler) throws Exception;

    /**
     * @param globalID 从db获取到的种子值
     * @param params   客户端传来sql内的参数,如user_id,restaurant_id,biz等
     */
    @Override public abstract String calc(long globalID, Map<String, String> params)
        throws SeqsException;

    /**
     * @param column 待解的字段名,如user_id。此字段的值在某些情况下可能被忽略。
     * @param value  可能为待解的id,如hongbao_id(123+1024*100,其中123为hash(user_id),100为从数据库中或去种子值)
     *               也可能为routingkey字符串, 如loc=113.5578632355,22.1942396955
     * @return 用于sharding分片的shardingId. 范围为0 ~ (ShardingConfig.HASH_RANGE - 1) , 失败返回-1
     */
    public abstract long decode(String column, String value);

    protected static final String ERR_MSG = ",please check globalid sql";

    protected static String getParam(Map<String, String> params, String key) throws SeqsException {
        String param = params.get(key);
        if (param == null) {
            throw new SeqsException(String.format("cannot find [%s] in params" + ERR_MSG, key));
        }
        return param;
    }

    public static boolean checkNumID(String id) {
        try {
            Long.parseLong(id);
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }

    public static long getHashCode(String id) {
        if (id == null)
            return -1;

        char[] value = id.toCharArray();
        long h = 0;
        if (h == 0 && value.length > 0) {
            char val[] = value;

            for (int i = 0; i < value.length; i++) {
                h = 31 * h + val[i];
            }
        }
        h = Math.abs(h);
        h = h % 1024;
        return h;
    }
}
