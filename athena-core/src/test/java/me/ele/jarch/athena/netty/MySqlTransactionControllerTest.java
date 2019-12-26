package me.ele.jarch.athena.netty;

import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Created by jinghao.wang on 2018/4/8.
 */
public class MySqlTransactionControllerTest {
    @Test public void testCanOpenTransaction() throws Exception {
        TransactionController transactionController = new MySqlTransactionController();
        assertFalse(transactionController.canOpenTransaction());
        // non autocommit模式事务
        transactionController.setAutoCommit(false);
        assertTrue(transactionController.canOpenTransaction());
        transactionController.setAutoCommit(true);
        assertFalse(transactionController.canOpenTransaction());

        // start transaction模式事务
        transactionController.markImplicitTransaction();
        assertTrue(transactionController.canOpenTransaction());
        transactionController.onTransactionEnd();
        assertFalse(transactionController.canOpenTransaction());

        // non autocommit + start transaction
        transactionController.setAutoCommit(false);
        transactionController.markImplicitTransaction();
        assertTrue(transactionController.canOpenTransaction());

        // non autocommit + start transaction end
        transactionController.onTransactionEnd();
        assertTrue(transactionController.canOpenTransaction());
    }

    @Test public void testSetAutoCommit() throws Exception {
        TransactionController transactionController = new MySqlTransactionController();
        assertTrue(transactionController.isAutoCommit());
        transactionController.setAutoCommit(false);
        assertFalse(transactionController.isAutoCommit());
    }

    @Test public void testMarkImplicitTransaction() throws Exception {
        TransactionController transactionController = new MySqlTransactionController();
        assertFalse(transactionController.hasImplicitTransactionMarker());
        transactionController.markImplicitTransaction();
        assertTrue(transactionController.hasImplicitTransactionMarker());
    }

    @Test public void testOnTransactionEnd() throws Exception {
        TransactionController transactionController = new MySqlTransactionController();
        transactionController.markImplicitTransaction();
        assertTrue(transactionController.hasImplicitTransactionMarker());
        transactionController.onTransactionEnd();
        assertFalse(transactionController.hasImplicitTransactionMarker());
    }

}
