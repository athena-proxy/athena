package me.ele.jarch.athena.seqs;

import me.ele.jarch.athena.sharding.ShardingRouter;
import me.ele.jarch.athena.sharding.sql.Send2BatchCond;
import me.ele.jarch.athena.sharding.sql.ShardingSQL;
import me.ele.jarch.athena.sql.CmdQuery;
import me.ele.jarch.athena.sql.seqs.SeqsException;
import me.ele.jarch.athena.sql.seqs.SeqsGenerator;
import me.ele.jarch.athena.sql.seqs.SeqsHandler;
import me.ele.jarch.athena.sql.seqs.SeqsParse;
import me.ele.jarch.athena.sql.seqs.generator.Generator;
import me.ele.jarch.athena.util.etrace.DalMultiMessageProducer;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * Created by xczhang on 15/8/10 下午2:16.
 */
public class SeqsTest {

    private static void parseSQL(CmdQuery cmd) {
        cmd.shardingSql = ShardingSQL
            .handleMySQLBasicly(cmd.query, "", ShardingRouter.defaultShardingRouter,
                new Send2BatchCond(), ShardingSQL.BLACK_FIELDS_FILTER);
        ShardingSQL.handleSQLSharding(cmd.shardingSql);
    }

    private void testCommonSeq(SeqsParse seqsParse, CmdQuery cmd) throws Exception {
        cmd.query =
            "SELECT next_value FROM dal_dual WHERE seq_name = 'common_seq' AND biz = 'hongbao'";
        parseSQL(cmd);
        seqsParse.parseGlobalIDSQL(cmd);
        Generator generator = SeqsGenerator.getGenerator(seqsParse.seqName);
        assertEquals(generator.calc(100, seqsParse.params), "100");
        assertEquals(generator.decode("hongbao", "100"), Generator.getHashCode("100"));
    }

    private void testCommonSeqExceptionWhenMoreParameters(SeqsParse seqsParse, CmdQuery cmd)
        throws Exception {
        cmd.query =
            "SELECT next_value FROM dal_dual WHERE seq_name = 'common_seq' AND biz = 'hongbao' AND id = 20";
        parseSQL(cmd);
        seqsParse.parseGlobalIDSQL(cmd);
        Generator generator = SeqsGenerator.getGenerator(seqsParse.seqName);
        try {
            generator.getGlobalID(new SeqsHandler(1, seqsParse.seqName, "eos",
                DalMultiMessageProducer.createEmptyProducer(), seqsParse.params, false, false) {

                @Override public void clientWriteGlobalID(byte[][] globalIDData, boolean success,
                    String errMsg) {
                }
            });
            Assert.fail();
        } catch (SeqsException e) { // NOSONAR
            Assert.assertTrue(e.getMessage().contains("too many useless params"));
        }
    }

    private void testComposedSeq(SeqsParse seqsParse, CmdQuery cmd) throws Exception {
        cmd.query =
            "SELECT next_value FROM dal_dual WHERE seq_name = 'composed_seq' AND biz = 'hongbao' AND hongbao_id = 123";
        parseSQL(cmd);
        seqsParse.parseGlobalIDSQL(cmd);
        Generator generator = SeqsGenerator.getGenerator(seqsParse.seqName);
        long generated_id = Generator.getHashCode("123") + 100 * 1024;
        assertEquals(generator.calc(100, seqsParse.params), String.valueOf(generated_id));
        assertEquals(generator.decode("", String.valueOf(generated_id)),
            Generator.getHashCode("123"));
    }

    /**
     * 利用反射设置一些初始值,并且使用mock的SeqsCache防止其真正连接db
     */
    @Test public void testGlobalId() throws Exception {
        SeqsParse seqsParse = new SeqsParse();
        Send2BatchCond batchCond = new Send2BatchCond();
        CmdQuery cmd = new CmdQuery(null, "eosgroup");
        cmd.batchCond = batchCond;
        cmd.table = "dal_dual";
        // 测试正常的common_seq情况
        testCommonSeq(seqsParse, cmd);

        // 测试在common_seq的情况,添加更多的冗余参数信息而抛异常
        testCommonSeqExceptionWhenMoreParameters(seqsParse, cmd);

        // 测试正常的composed_seq情况
        testComposedSeq(seqsParse, cmd);
    }
}
