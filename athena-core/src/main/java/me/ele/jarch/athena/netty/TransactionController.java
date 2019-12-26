package me.ele.jarch.athena.netty;

/**
 * Created by jinghao.wang on 2018/4/3.
 */
public abstract class TransactionController {
    protected boolean implictTransactionMarker = false;

    public TransactionController() {
    }

    /**
     * 是否有能力开启事务
     * MySQL协议: 由连接autoCommit属性和是否发送了START TRANSACTION共同决定
     * PostgreSQL协议: 由是否发送了START TRANSACTION决定
     *
     * @return true 在写类型SQL时可以打开事务，false 在写类型SQL时只能使用autoCommit模式.
     */
    public abstract boolean canOpenTransaction();

    /**
     * 当前连接是否是autocommit类型
     * MySQL协议: 取决与Client的设置值，默认为true
     * PostgreSQL协议: 永远返回true
     *
     * @return
     */
    public abstract boolean isAutoCommit();

    /**
     * 设置当前连接的autocommit类型
     * MySQL协议: 改变autoCommit属性
     * PostgreSQL协议: 什么也不做(此方法不应该被调用)
     *
     * @param autoCommit
     */
    public abstract void setAutoCommit(boolean autoCommit);

    /**
     * Client是否发送过START TRANSACTION或BEGIN SQL, 且还没有收到对应的COMMIT/ROLLBACK
     *
     * @return
     */
    public boolean hasImplicitTransactionMarker() {
        return implictTransactionMarker;
    }

    /**
     * 标记收到了Client发来的START TRANSACTION或BEGIN SQL
     */
    public void markImplicitTransaction() {
        implictTransactionMarker = true;
    }

    /**
     * 在COMMIT/ROLLBACK执行完成后，清除隐式事务标记(ImplicitTransactionMarker)
     */
    public void onTransactionEnd() {
        implictTransactionMarker = false;
    }
}
