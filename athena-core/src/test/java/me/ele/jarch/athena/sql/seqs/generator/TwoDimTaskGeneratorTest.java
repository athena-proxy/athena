package me.ele.jarch.athena.sql.seqs.generator;

import me.ele.jarch.athena.sql.seqs.GeneratorUtil;
import me.ele.jarch.athena.sql.seqs.SeqsException;
import me.ele.jarch.athena.sql.seqs.SeqsHandler;
import me.ele.jarch.athena.util.etrace.DalMultiMessageProducer;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.*;

/**
 * Created by jinghao.wang on 2017/10/31.
 */
public class TwoDimTaskGeneratorTest {
    private final TwoDimTaskGenerator contractTaskGenerator =
        new TwoDimTaskGenerator(GeneratorUtil.PREFIX, GeneratorUtil.FIRST_DIM,
            GeneratorUtil.SECOND_DIM);

    @Test public void testCalc() throws SeqsException {
        Map<String, String> params = new HashMap<>();
        params.put(GeneratorUtil.FIRST_DIM, "123");
        params.put(GeneratorUtil.SECOND_DIM, "456");
        params.put(GeneratorUtil.PREFIX, "26383326abc");
        String taskId = contractTaskGenerator.calc(0, params);
        assertEquals(taskId, "26383326abc8c9d5");
    }

    @Test public void testCalc27Prefix() throws SeqsException {
        Map<String, String> params = new HashMap<>();
        params.put(GeneratorUtil.FIRST_DIM, "123");
        params.put(GeneratorUtil.SECOND_DIM, "456");
        params.put(GeneratorUtil.PREFIX, "26383326abc2681268er26defa1");
        String taskId = contractTaskGenerator.calc(0, params);
        assertEquals(taskId, "26383326abc2681268er26defa18c9d5");
    }

    @Test public void testCalc28Prefix() throws SeqsException {
        Map<String, String> params = new HashMap<>();
        params.put(GeneratorUtil.FIRST_DIM, "123");
        params.put(GeneratorUtil.SECOND_DIM, "456");
        params.put(GeneratorUtil.PREFIX, "26383326abc2681268er26defa12");
        String taskId = contractTaskGenerator.calc(0, params);
        assertEquals(taskId, "26383326abc2681268er26defa18c9d5");
    }

    @Test public void testCalcBuyerIdHashSmall() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put(GeneratorUtil.FIRST_DIM, "1230");
        params.put(GeneratorUtil.SECOND_DIM, "456");
        params.put(GeneratorUtil.PREFIX, "26383326abc");
        String taskId = contractTaskGenerator.calc(0, params);
        assertEquals(taskId, "26383326abc0f9d5");
    }

    @Test(expectedExceptions = SeqsException.class, expectedExceptionsMessageRegExp = GeneratorUtil.PREFIX)
    public void testCalcMissPrefix() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put(GeneratorUtil.FIRST_DIM, "1230");
        params.put(GeneratorUtil.SECOND_DIM, "456");
        contractTaskGenerator.calc(0, params);
        fail();
    }

    @Test(expectedExceptions = SeqsException.class, expectedExceptionsMessageRegExp = GeneratorUtil.FIRST_DIM)
    public void testCalcMissBuyerId() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put(GeneratorUtil.SECOND_DIM, "456");
        params.put(GeneratorUtil.PREFIX, "26383326abc");
        contractTaskGenerator.calc(0, params);
        fail();
    }

    @Test(expectedExceptions = SeqsException.class, expectedExceptionsMessageRegExp = GeneratorUtil.SECOND_DIM)
    public void testCalcMissSellerId() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put(GeneratorUtil.FIRST_DIM, "1230");
        params.put(GeneratorUtil.PREFIX, "26383326abc");
        contractTaskGenerator.calc(0, params);
        fail();
    }

    @Test public void testGetGlobalID() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put(GeneratorUtil.SEQ_NAME, GeneratorUtil.FIX_HASH_SEQ);
        params.put(GeneratorUtil.FIRST_DIM, "123");
        params.put(GeneratorUtil.SECOND_DIM, "456");
        params.put(GeneratorUtil.PREFIX, "26383326abc");
        SeqsHandler seqsHandler = new SeqsHandler(0, GeneratorUtil.FIX_HASH_SEQ, "abc_test_group",
            DalMultiMessageProducer.createEmptyProducer(), params, false, false) {
            @Override
            public void clientWriteGlobalID(byte[][] globalIDData, boolean success, String errMsg) {
                assertTrue(success);
            }
        };

        contractTaskGenerator.getGlobalID(seqsHandler);
    }

    @Test(expectedExceptions = SeqsException.class, expectedExceptionsMessageRegExp = "missing "
        + GeneratorUtil.PREFIX + ".*") public void testGetGlobalIDMissPrefix() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put(GeneratorUtil.SEQ_NAME, GeneratorUtil.FIX_HASH_SEQ);
        params.put(GeneratorUtil.FIRST_DIM, "123");
        params.put(GeneratorUtil.SECOND_DIM, "456");
        SeqsHandler seqsHandler = new SeqsHandler(0, GeneratorUtil.FIX_HASH_SEQ, "abc_test_group",
            DalMultiMessageProducer.createEmptyProducer(), params, false, false) {
            @Override
            public void clientWriteGlobalID(byte[][] globalIDData, boolean success, String errMsg) {
                fail();
            }
        };
        contractTaskGenerator.getGlobalID(seqsHandler);
        fail();
    }

    @Test(expectedExceptions = SeqsException.class, expectedExceptionsMessageRegExp = "missing "
        + GeneratorUtil.FIRST_DIM + ".*") public void testGetGlobalIDMissBuyerId()
        throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put(GeneratorUtil.SEQ_NAME, GeneratorUtil.FIX_HASH_SEQ);
        params.put(GeneratorUtil.SECOND_DIM, "456");
        params.put(GeneratorUtil.PREFIX, "26383326abc");
        SeqsHandler seqsHandler = new SeqsHandler(0, GeneratorUtil.FIX_HASH_SEQ, "abc_test_group",
            DalMultiMessageProducer.createEmptyProducer(), params, false, false) {
            @Override
            public void clientWriteGlobalID(byte[][] globalIDData, boolean success, String errMsg) {
                fail();
            }
        };
        contractTaskGenerator.getGlobalID(seqsHandler);
        fail();
    }

    @Test(expectedExceptions = SeqsException.class, expectedExceptionsMessageRegExp = "missing "
        + GeneratorUtil.SECOND_DIM + ".*") public void testGetGlobalIDMissSellerId()
        throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put(GeneratorUtil.SEQ_NAME, GeneratorUtil.FIX_HASH_SEQ);
        params.put(GeneratorUtil.FIRST_DIM, "123");
        params.put(GeneratorUtil.PREFIX, "26383326abc");
        SeqsHandler seqsHandler = new SeqsHandler(0, GeneratorUtil.FIX_HASH_SEQ, "abc_test_group",
            DalMultiMessageProducer.createEmptyProducer(), params, false, false) {
            @Override
            public void clientWriteGlobalID(byte[][] globalIDData, boolean success, String errMsg) {
                fail();
            }
        };
        contractTaskGenerator.getGlobalID(seqsHandler);
        fail();
    }

    @Test(expectedExceptions = SeqsException.class, expectedExceptionsMessageRegExp = "too many useless params.*")
    public void testGetGlobalIDMoreUselessParams() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put(GeneratorUtil.SEQ_NAME, GeneratorUtil.FIX_HASH_SEQ);
        params.put(GeneratorUtil.FIRST_DIM, "123");
        params.put(GeneratorUtil.SECOND_DIM, "456");
        params.put(GeneratorUtil.PREFIX, "26383326abc");
        params.put("abc", "sdghfisdhi");
        SeqsHandler seqsHandler = new SeqsHandler(0, GeneratorUtil.FIX_HASH_SEQ, "abc_test_group",
            DalMultiMessageProducer.createEmptyProducer(), params, false, false) {
            @Override
            public void clientWriteGlobalID(byte[][] globalIDData, boolean success, String errMsg) {
                fail();
            }
        };

        contractTaskGenerator.getGlobalID(seqsHandler);
        fail();
    }

    @Test public void testDecode() throws Exception {
        String buyerId = "123";
        String sellerId = "456";
        String contractTaskId = "26383326abc8c9d5";
        assertEquals(contractTaskGenerator.decode(GeneratorUtil.FIRST_DIM, contractTaskId),
            Generator.getHashCode(buyerId));
        assertEquals(contractTaskGenerator.decode(GeneratorUtil.SECOND_DIM, contractTaskId),
            Generator.getHashCode(sellerId));
    }

    @Test public void testDecodeIllegalTaskId() throws Exception {
        String illegal = "26383326abc8c9dz";
        assertEquals(contractTaskGenerator.decode(GeneratorUtil.FIRST_DIM, illegal), -1);
        assertEquals(contractTaskGenerator.decode(GeneratorUtil.SECOND_DIM, illegal), -1);
    }
}
