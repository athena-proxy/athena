package me.ele.jarch.athena.sql;

public enum QUERY_TYPE {
    //@formatter:off
    SELECT,
    UPDATE,
    REPLACE,
    INSERT,
    DELETE,
    SELECT_FOR_UPDATE,
    COMMIT,
    RELEASE,
    SAVEPOINT,
    ROLLBACK,
    ROLLBACK_TO,
    IGNORE_CMD,
    SHOW_CMD,
    SET_AUTOCOMMIT,
    OTHER,
    TRUNCATE,
    MULTI_TRANS,
    MULTI_NONE_TRANS,
    KILL_CMD,
    JDBC_PREPARE_STMT,
    // 有一些需要伪造结果集的命令,比如show warnings
    // 该种命令返回的结果不是OK包,而是一个ResultSet,所以需要单独为其定义一个类型
    // 且为其添加isFakeResultStatement方法
    SHOW_WARNINGS,
    MYSQL_PING,
    PG_EMPTY_QUERY,
    // 由于DAL拦截掉了MySQL Autocommit相关逻辑,所以其设置查询逻辑都应该由DAL拦截掉
    // 以给Client提供一致的视角。以前DAL只拦截了SET逻辑,并未拦截查询逻辑。此类型为
    // 拦截查询逻辑新增
    QUERY_AUTOCOMMIT,
    // BEGIN 类型源于 PG的`BEGIN` sql或MySQL `START TRANSACTION` sql。BEGIN类型在2种协议中的处理是不同的。
    BEGIN,
    // PG specific SQL Type
    COPY,
    DO,
    /**
     * PG SYNC命令有以下行为:
     * <p>
     * See <a href="https://www.postgresql.org/docs/9.4/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY">Extended Query</a>
     * <p>
     * JDBC(只看过9.4.1212)对此有特殊行为:
     * <p>
     * See <a href="https://github.com/pgjdbc/pgjdbc/blob/a16141bb60df1ac9a85279c31a23011ea8614669/pgjdbc/src/main/java/org/postgresql/core/v3/QueryExecutorImpl.java#L362">Deadlock avoidance</a>
     * <pre>
     *     MAX_BUFFERED_RECV_BYTES / NODATA_QUERY_RESPONSE_SIZE_BYTES = 256
     * </pre>
     * 如果先调用prepareStatement(),再调用addBatch()很多次(大于256次),最后调用executeBatch();
     * <p>
     * JDBC会首先发送255条(参数preparedStatementCacheQueries默认值)SQL,然后发送一个SYNC命令
     * <p>
     * 接着JDBC会尝试等待接收255个CommandComplete和255个ReadyForQuery(交替),最后再收到一个(SYNC返回的结果)ReadyForQuery
     * <p>
     * 等待以上结果全部收完之后,判断是否所有结果都OK,只有全部OK才会接续发送SYNC之后所余下的addBatch的SQL, 否则提前结束(SYNC后不会再发SQL)
     * <p>
     * 也就是说, SYNC是用来提供一个错误恢复的重新同步的点
     * <p>
     * 所以该命令是发往MASTER库的,且并不会关闭用BEGIN打开的事务
     * <p>
     * 鉴于上述特性,把它归类为和DO命令相同的语句类型
     */
    SYNC;
    //@formatter:on

    /**
     * 判断该SQL是否为写(修改)数据操作的语句类型
     */
    public boolean isWritableStatement() {
        switch (this) {
            case UPDATE:
            case REPLACE:
            case INSERT:
            case DELETE:
            case SELECT_FOR_UPDATE:
            case MULTI_TRANS:
            case DO:
            case SYNC:
                return true;
            default:
                return false;
        }
    }

    // for GrayState
    public boolean isTransStatement() {
        return this == SELECT_FOR_UPDATE || this == UPDATE || this == REPLACE || this == INSERT
            || this == DELETE || this == COMMIT || this == ROLLBACK || this == MULTI_TRANS;
    }

    /**
     * 判断该SQL是否为只读的语句类型
     */
    public boolean isReadOnlyStatement() {
        return this == SELECT || this == SHOW_CMD || this == MULTI_NONE_TRANS;
    }

    public boolean isEndTrans() {
        return this == COMMIT || this == ROLLBACK;
    }

    public boolean isForSavePoint() {
        return this == ROLLBACK_TO || this == RELEASE || this == SAVEPOINT;
    }

    public boolean isUnknownTrans() {
        return this == OTHER;
    }

    public boolean isFakeResultStatement() {
        return this == SHOW_WARNINGS || this == QUERY_AUTOCOMMIT || this == MYSQL_PING
            || this == PG_EMPTY_QUERY;
    }
}
