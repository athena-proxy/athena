package me.ele.jarch.athena.netty;

/**
 * Created by jinghao.wang on 2018/4/3.
 */
public class PGTransactionController extends TransactionController {
    public PGTransactionController() {
    }

    @Override public boolean canOpenTransaction() {
        return implictTransactionMarker;
    }

    @Override public boolean isAutoCommit() {
        return true;
    }

    @Override public void setAutoCommit(boolean autoCommit) {
        // NOOP
    }
}
