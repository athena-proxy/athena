package me.ele.jarch.athena.netty;

/**
 * Created by jinghao.wang on 2018/4/3.
 */
public class MySqlTransactionController extends TransactionController {
    private boolean autoCommit = true;

    public MySqlTransactionController() {
    }

    @Override public boolean canOpenTransaction() {
        return implictTransactionMarker || !autoCommit;
    }

    @Override public boolean isAutoCommit() {
        return autoCommit;
    }

    @Override public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }
}
