package me.ele.jarch.athena.util.etrace;

import org.testng.annotations.Test;

import java.util.Objects;

import static org.testng.Assert.*;

/**
 * Created by jinghao.wang on 17/6/27.
 */
public class DalMultiMessageProducerTest {

    @Test public void testCreateEmptyProducer() throws Exception {
        DalMultiMessageProducer noop = DalMultiMessageProducer.createEmptyProducer();
        DalMultiMessageProducer noop1 = DalMultiMessageProducer.createEmptyProducer();
        assertTrue(noop == noop1);
        assertEquals(noop, DalMultiMessageProducer.NOOP);
        assertTrue(noop.isEmpty());
    }

    @Test public void testStartTransactionAndGet() throws Exception {
        DalMultiMessageProducer noop = DalMultiMessageProducer.createEmptyProducer();
        DalMultiMessageProducer real = DalMultiMessageProducer.createProducer();
        assertEquals(noop.startTransactionAndGet("DAL", "test.select"), null);
        assertTrue(Objects.nonNull(real.startTransactionAndGet("DAL", "test.select")));
    }

    @Test public void testIsEmpty() throws Exception {
        DalMultiMessageProducer noop = DalMultiMessageProducer.createEmptyProducer();
        DalMultiMessageProducer real = DalMultiMessageProducer.createProducer();
        assertTrue(noop.isEmpty());
        assertFalse(real.isEmpty());
    }

    @Test public void testNextLocalRpcId() throws Exception {
        DalMultiMessageProducer noop = DalMultiMessageProducer.createEmptyProducer();
        DalMultiMessageProducer real = DalMultiMessageProducer.createProducer();
        assertEquals(noop.nextLocalRpcId(), "");
        assertFalse(Objects.equals(real.nextLocalRpcId(), ""));
    }

    @Test public void testCurrentRequestId() throws Exception {
        DalMultiMessageProducer noop = DalMultiMessageProducer.createEmptyProducer();
        DalMultiMessageProducer real = DalMultiMessageProducer.createProducer();
        assertEquals(noop.currentRequestId(), "");
        assertFalse(Objects.equals(real.currentRequestId(), ""));
    }

}
