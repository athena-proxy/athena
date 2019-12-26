package me.ele.jarch.athena.seqs;

import me.ele.jarch.athena.allinone.DBConnectionInfo;
import me.ele.jarch.athena.allinone.DBRole;
import me.ele.jarch.athena.exception.QueryException;
import me.ele.jarch.athena.sharding.ShardingRouter;
import me.ele.jarch.athena.sharding.sql.Send2BatchCond;
import me.ele.jarch.athena.sharding.sql.ShardingSQL;
import me.ele.jarch.athena.sql.CmdQuery;
import me.ele.jarch.athena.sql.seqs.*;
import me.ele.jarch.athena.sql.seqs.generator.Generator;
import me.ele.jarch.athena.util.etrace.DalMultiMessageProducer;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;

import static org.testng.Assert.assertEquals;

/**
 * Created by avenger on 2016/11/16.
 */
public class RangeSeqsTest {
    private static final long MIN_19_ORDERID_SEED = 119209289551L;

    @Test public void testGlobalId() throws Exception {
        SeqsParse seqsParse = new SeqsParse();
        CmdQuery cmd = new CmdQuery(null, "eosgroup");
        cmd.table = "dal_dual";
        // 测试正常的common_seq情况
        cmd.query =
            "SELECT next_value FROM dal_dual WHERE seq_name = 'common_seq' AND biz = 'hongbao'";
        parseSQL(cmd);
        seqsParse.parseGlobalIDSQL(cmd);
        Generator generator = SeqsGenerator.getGenerator(seqsParse.seqName);
        assertEquals(generator.calc(100, seqsParse.params), "100");
        assertEquals(generator.decode("hongbao", "100"), Generator.getHashCode("100"));
    }

    @Test public void testRangeGlobalId() throws Exception {
        SeqsParse seqsParse = new SeqsParse();
        CmdQuery cmd = new CmdQuery(null, "eosgroup");
        cmd.table = "dal_dual";
        // 测试正常的common_seq情况
        cmd.query =
            "SELECT next_begin, next_end FROM dal_dual WHERE seq_name = 'common_seq' AND biz = 'hongbao' and count = 10";
        parseSQL(cmd);
        seqsParse.parseGlobalIDSQL(cmd);
        SeqsCacheData seqsCacheData = new SeqsCacheData();
        AsyncGlobalID asyncGlobalID = new AsyncGlobalID(mockDBConn(), "common_seq", seqsCacheData);

        //用来区分第几次测试
        final AtomicInteger times = new AtomicInteger(0);
        SeqsHandler seqsHandler = new SeqsHandler(1, seqsParse.seqName, "eos",
            DalMultiMessageProducer.createEmptyProducer(), seqsParse.params, true, true) {
            @Override
            public void clientWriteGlobalID(byte[][] globalIDData, boolean success, String errMsg) {
                switch (times.get()) {
                    case 0:
                        assertEquals(byteEquals(getSeqsBytes(101, 110, true), globalIDData), true);
                        break;
                    case 1:
                        assertEquals(byteEquals(getSeqsBytes(111, 120, true), globalIDData), true);
                        break;
                    default:
                        break;
                }
            }

        };

        CacheTestCase cacheTestCase = new CacheTestCase(asyncGlobalID);

        //初始化缓存
        cacheTestCase.setMockCache(seqsCacheData, 100, 100);

        assertEquals(seqsCacheData.getSeq_id_Base(), 100L);
        assertEquals(seqsCacheData.getSeqID(), 0L);
        assertEquals(seqsCacheData.getCache_size(), 100);

        //第一次获取10个Global ID
        cacheTestCase.getSeqValue(seqsHandler);
        times.incrementAndGet();

        assertEquals(seqsCacheData.getSeq_id_Base(), 100L);
        assertEquals(seqsCacheData.getSeqID(), 10L);
        assertEquals(seqsCacheData.getCache_size(), 100);

        //第二次获取10个Global ID
        cacheTestCase.getSeqValue(seqsHandler);
        times.incrementAndGet();

        assertEquals(seqsCacheData.getSeq_id_Base(), 100L);
        assertEquals(seqsCacheData.getSeqID(), 20L);
        assertEquals(seqsCacheData.getCache_size(), 100);

        final AtomicInteger isCalled = new AtomicInteger(0);
        seqsParse.params.put("count", "100");
        SeqsHandler seqsHandler2 = new SeqsHandler(1, seqsParse.seqName, "eos",
            DalMultiMessageProducer.createEmptyProducer(), seqsParse.params, true, true) {
            @Override
            public void clientWriteGlobalID(byte[][] globalIDData, boolean success, String errMsg) {
                isCalled.incrementAndGet();
            }

        };
        //第三次获取100个Global ID ,由于没有实际的DB回调,因此缓存值将没有改变
        cacheTestCase.getSeqValue(seqsHandler2);
        assertEquals(seqsCacheData.getSeq_id_Base(), 100L);
        assertEquals(seqsCacheData.getSeqID(), 20L);
        assertEquals(seqsCacheData.getCache_size(), 100);
        assertEquals(isCalled.get(), 0);



    }

    @Test public void testBorder1SerialGlobalId() throws Exception {
        SeqsParse seqsParse = new SeqsParse();
        CmdQuery cmd = new CmdQuery(null, "eosgroup");
        cmd.table = "dal_dual";
        // 测试正常的common_seq情况
        cmd.query =
            "SELECT next_begin, next_end FROM dal_dual WHERE seq_name = 'common_seq' AND biz = 'hongbao' and count = 1";
        parseSQL(cmd);
        seqsParse.parseGlobalIDSQL(cmd);
        SeqsCacheData seqsCacheData = new SeqsCacheData();
        AsyncGlobalID asyncGlobalID = new AsyncGlobalID(mockDBConn(), "common_seq", seqsCacheData);

        //用来区分第几次测试
        final AtomicInteger times = new AtomicInteger(0);
        SeqsHandler seqsHandler = new SeqsHandler(1, seqsParse.seqName, "eos",
            DalMultiMessageProducer.createEmptyProducer(), seqsParse.params, true, true) {
            @Override
            public void clientWriteGlobalID(byte[][] globalIDData, boolean success, String errMsg) {
                switch (times.get()) {
                    case 0:
                        assertEquals(byteEquals(getSeqsBytes(101, 101, true), globalIDData), true);
                        break;
                    default:
                        break;
                }
            }

        };

        CacheTestCase cacheTestCase = new CacheTestCase(asyncGlobalID);

        //初始化缓存
        cacheTestCase.setMockCache(seqsCacheData, 100, 100);
        //第一次获取10个Global ID
        cacheTestCase.getSeqValue(seqsHandler);
        times.incrementAndGet();

    }

    @Test public void testBorder1Serial2GlobalId() throws Exception {
        SeqsParse seqsParse = new SeqsParse();
        CmdQuery cmd = new CmdQuery(null, "eosgroup");
        cmd.table = "dal_dual";
        // 测试正常的common_seq情况
        cmd.query =
            "SELECT next_value FROM dal_dual WHERE seq_name = 'common_seq' AND biz = 'hongbao' and count = 1";
        parseSQL(cmd);
        seqsParse.parseGlobalIDSQL(cmd);
        SeqsCacheData seqsCacheData = new SeqsCacheData();
        AsyncGlobalID asyncGlobalID = new AsyncGlobalID(mockDBConn(), "common_seq", seqsCacheData);

        //用来区分第几次测试
        final AtomicInteger times = new AtomicInteger(0);
        SeqsHandler seqsHandler = new SeqsHandler(1, seqsParse.seqName, "eos",
            DalMultiMessageProducer.createEmptyProducer(), seqsParse.params, true, true) {
            @Override
            public void clientWriteGlobalID(byte[][] globalIDData, boolean success, String errMsg) {
                switch (times.get()) {
                    case 0:
                        assertEquals(byteEquals(getSeqsBytes(101, 101, true), globalIDData), true);
                        break;
                    default:
                        break;
                }
            }

        };

        CacheTestCase cacheTestCase = new CacheTestCase(asyncGlobalID);

        //初始化缓存
        cacheTestCase.setMockCache(seqsCacheData, 100, 100);
        //第一次获取10个Global ID
        cacheTestCase.getSeqValue(seqsHandler);
        times.incrementAndGet();

    }

    @Test public void testBorder0GlobalId() throws Exception {
        SeqsParse seqsParse = new SeqsParse();
        CmdQuery cmd = new CmdQuery(null, "eosgroup");
        cmd.table = "dal_dual";
        // 测试正常的common_seq情况
        cmd.query =
            "SELECT next_value FROM dal_dual WHERE seq_name = 'common_seq' AND biz = 'hongbao' and count = 0";
        parseSQL(cmd);
        seqsParse.parseGlobalIDSQL(cmd);
        try {
            seqsParse.checkSeqParamCount();
        } catch (QueryException e) {
            assertEquals(e.getMessage().contains("global id sql count must be in"), true);
        }
    }

    @Test public void testBorder1001GlobalId() throws Exception {
        SeqsParse seqsParse = new SeqsParse();
        CmdQuery cmd = new CmdQuery(null, "eosgroup");
        cmd.table = "dal_dual";
        // 测试正常的common_seq情况
        cmd.query =
            "SELECT next_value FROM dal_dual WHERE seq_name = 'common_seq' AND biz = 'hongbao' and count = 1001";
        parseSQL(cmd);
        seqsParse.parseGlobalIDSQL(cmd);
        try {
            seqsParse.checkSeqParamCount();
        } catch (QueryException e) {
            assertEquals(e.getMessage().contains("global id sql count must be in"), true);
        }
    }


    private static void parseSQL(CmdQuery cmd) {
        cmd.shardingSql = ShardingSQL
            .handleMySQLBasicly(cmd.query, "", ShardingRouter.defaultShardingRouter,
                new Send2BatchCond(), ShardingSQL.BLACK_FIELDS_FILTER);
        ShardingSQL.handleSQLSharding(cmd.shardingSql);
    }

    private static DBConnectionInfo mockDBConn() {
        DBConnectionInfo info = new DBConnectionInfo();
        info.setGroup("eos_group");
        info.setDatabase("daltestdb");
        info.setHost("192.168.80.19");
        info.setPort(3308);
        info.setUser("root");
        info.setPword("root");
        info.setRole(DBRole.MASTER);
        info.setId("master0");
        info.setAlive(true);
        info.setReadOnly(false);
        return info;
    }


    public class CacheTestCase {
        public AsyncGlobalID asyncGlobalID;

        public CacheTestCase(AsyncGlobalID asyncGlobalID) {
            this.asyncGlobalID = asyncGlobalID;
        }

        public void getSeqValue(SeqsHandler handler) throws Exception {
            asyncGlobalID.offer(handler);
        }

        /**
         * 模拟调用 seqsCacheData 的 appendSeqSeed, 来初始化缓存
         *
         * @param base
         * @param cacheSize
         * @throws Exception
         */
        public void setMockCache(SeqsCacheData seqsCacheData, long base, int cacheSize)
            throws Exception {
            seqsCacheData.appendSeqSeed(base, cacheSize, null);
        }

    }

    public static byte[][] getSeqsBytes(long start, long end, boolean isSerial) {
        List<String> resultList = new ArrayList<>();

        if (isSerial) {
            resultList.add(String.valueOf(start));
            resultList.add(String.valueOf(end));
            return GeneratorUtil.getSerialIDPackets("next_begin", "next_end", resultList)
                .toArray(new byte[0][]);
        } else {
            LongStream.rangeClosed(start, end).forEach(i -> resultList.add(String.valueOf(i)));
            return GeneratorUtil.getIDPackets("next_value", resultList).toArray(new byte[0][]);
        }
    }

    public static byte[][] getOrderIdSeqsBytes(SeqsParse seqsParse, long start, long end) {
        try {
            Generator generator = SeqsGenerator.getGenerator(seqsParse.seqName);
            List<String> resultList = new ArrayList<>();
            for (long i = start; i <= end; i++) {
                resultList.add(generator.calc(i, seqsParse.params));
            }
            return GeneratorUtil.getIDPackets("next_value", resultList).toArray(new byte[0][]);
        } catch (Exception e) {
            return null;
        }

    }

    /**
     * 比较二维数组是否相等
     *
     * @param b1
     * @param b2
     * @return
     */
    public static boolean byteEquals(byte[][] b1, byte[][] b2) {
        if (b1 == null || b2 == null) {
            return false;
        }
        if (b1.length != b2.length) {
            return false;
        }
        for (int i = 0; i < b1.length; i++) {
            if (b1[i] == null || b2[i] == null) {
                return false;
            }
            if (!Arrays.equals(b1[i], b2[i])) {
                return false;
            }
        }
        return true;
    }
}
