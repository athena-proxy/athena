package me.ele.jarch.athena.netty;

import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Created by jinghao.wang on 2018/4/8.
 */
public class PGTransactionControllerTest {
    @Test public void testCanOpenTransaction() throws Exception {
        TransactionController transactionController = new PGTransactionController();
        assertFalse(transactionController.canOpenTransaction());
        transactionController.setAutoCommit(false);
        assertFalse(transactionController.canOpenTransaction());
        transactionController.markImplicitTransaction();
        assertTrue(transactionController.canOpenTransaction());
        transactionController.onTransactionEnd();
        assertFalse(transactionController.canOpenTransaction());
    }

    @Test public void testSetAutoCommit() throws Exception {
        TransactionController transactionController = new PGTransactionController();
        assertTrue(transactionController.isAutoCommit());
        transactionController.setAutoCommit(false);
        assertTrue(transactionController.isAutoCommit());
    }

    @Test public void testMarkImplicitTransaction() throws Exception {
        TransactionController transactionController = new PGTransactionController();
        assertFalse(transactionController.hasImplicitTransactionMarker());
        transactionController.markImplicitTransaction();
        assertTrue(transactionController.hasImplicitTransactionMarker());
    }

    @Test public void testOnTransactionEnd() throws Exception {
        TransactionController transactionController = new PGTransactionController();
        transactionController.markImplicitTransaction();
        assertTrue(transactionController.hasImplicitTransactionMarker());
        transactionController.onTransactionEnd();
        assertFalse(transactionController.hasImplicitTransactionMarker());
    }

}
