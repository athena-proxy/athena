package me.ele.jarch.athena.constant;

public class Metrics {
    /* Duration */
    public static final String DURATION_PROXY = "time.proxy.duration";
    public static final String DURATION_CPROXY = "time.cproxy.duration";
    public static final String DURATION_SERVER = "time.server.duration";
    public static final String DURATION_COMMIT = "time.commit.duration";

    /* gray */
    public static final String DURATION_GRAY_PROXY = "gray.time.proxy.duration";
    public static final String DURATION_GRAY_SERVER = "gray.time.server.duration";

    /* sharding */
    public static final String AFFECT_ROWS_DIFF = "sharding.affect.row.diff";
    public static final String SECOND_COMMIT_FAIL = "sharding.second.commit.failure";
    /* 二维sharding，有一维度commit失败*/
    public static final String SOMEONE_COMMIT_FAIL = "sharding.someone.commit.failure";

    /* Mapping sharding */
    public static final String MAPPING_SQL_AFFECTED_ZERO = "sharding.mapping.sql.affectedzero";
    public static final String MAPPING_SQL_FAIL = "sharding.mapping.sql.failure";

    /* TPS,QPS */
    public static final String TRANS_TPS = "transaction.tps";
    public static final String TRANS_QPS = "transaction.qps";
    public static final String TRANS_CTPS = "transaction.ctps";
    public static final String TRANS_CQPS = "transaction.cqps";
    /* TPS,QPS END */

    // for global id
    public static final String GLOBALID_SERVER_BROKEN = "sql.globalid_server_broken";
    public static final String GLOBALID_CLIENT_BROKEN = "sql.globalid_client_broken";
    public static final String TIME_GLOBALID_DURATION = "time.globalid.duration";
    // DAL从DB获取种子成功
    public static final String TRANS_GLOBALID_TPS = "transaction.globalid_server_tps";
    // client从DAL获取global id成功
    public static final String TRANS_GLOBALID_QPS = "transaction.globalid_client_qps";

    // AutoKiller
    public static final String AK_KILL_SQL = "autokill.kill.sql";
    public static final String AK_KILL_TRAN = "autokill.kill.tran";
    public static final String AK_PASSED = "autokill.passed";
    // DB sick
    public static final String SICK_KILL = "sql.sick";

    public static final String CONN_CLIENT_BROKEN = "connection.client.broken";
    public static final String CONN_SERVER_BROKEN = "connection.server.broken";

    public static final String SQL_DBERROR = "sql.error";
    public static final String SQL_DALERROR = "sql.dalerror";
    public static final String SQL_ABORT = "sql.abort";
    public static final String SQL_CROSS_EZONE = "sql.crossezone";
    // SQL执行影响行数，目前记录DELETE,UPDATE类型的影响行数
    public static final String SQL_AFFECTED = "sql.affected";

    // for multi datacenter
    public static final String SQL_CROSS_EZONE_WARN = "sql.cross_ezone_warn";
    public static final String SQL_CROSS_EZONE_REJECT = "sql.cross_ezone_reject";
    public static final String SQL_REBALANCE = "sql.rebalance";
    public static final String SQL_EZONE_NOT_AVAILABLE_REJECT = "sql.ezone_not_available_reject";

    public static final String HANG_SQL = "hang.sql";
    public static final String HANG_COMMIT = "hang.commit";
    public static final String LONG_LIVED_SESSION = "session.long";
    public static final String SESSION_LIVE_TIME = "session.age";

    public static final String REJECT_SQL = "reject.sql";
    public static final String COMMIT_FAILURE = "commit.failure";

    public static final String LOGIN_MYSQL_CLIENT = "login.mysql.client";
    public static final String LOGIN_SLOW = "client.login.slow";
    public static final String LOGIN_TIMEOUT = "client.login.timeout";
    public static final String LOGIN_SUSPECT_PING = "client.suspect.ping";

    public static final String SCHEDULER_QUEUE_SIZE = "scheduler.queue.size";
    public static final String SEMAPHORE_EMPTY = "semaphore.empty";

    //busniss worker queue size
    public static final String WORKER_QUEUE_SIZE = "worker.queue.size";
    //single netty worker tasks
    public static final String NETTY_QUEUE_SIZE = "netty.queue.size";
    //mulscheduler registered queues size
    public static final String MULSCHEDULER_QUEUE_SIZE = "mulscheduler.queue.size";

    public static final String HB_SRV_UP = "heartbeat.serviceup";
    public static final String HB_SRV_DOWN = "heartbeat.servicedown";
    public static final String HB_FAILED = "heartbeat.failed";
    public static final String HB_NO_AVAILABLE_MASTER = "heartbeat.no_available_master";
    public static final String HB_MULTI_AVAILABLE_MASTER = "heartbeat.multi_available_master";
    public static final String HB_MASTER_SWITCHED = "heartbeat.master_switched";
    public static final String SHOW_SLAVE_FAILED = "heartbeat.show_slave_failed";

    //config related
    public static final String CONFIG_ERROR = "config.error";
    public static final String SHARDING_CONFIG_ERROR = "sharding.config.error";
    public static final String CONFIG_OUTDATED = "config.outdated";
    public static final String CONFIG_ZK_CHANGED = "config.zk.changed";

    // sql pattern too much, metric for alert
    public static final String SQLPATTERN_TOO_MUCH = "sqlpattern.too.much";

    public static final String GLOBALID_DISALLOWED_DALGROUP = "globalid.disallowed_dalgroup";

    public static final String GZS_INIT_ERROR = "gzs.init_error";
    public static final String GZS_ZERO_SHARDID = "gzs.zero_shardid";
    public static final String GZS_REMOTE_QUERY = "gzs.remote_query";

    //GZS internal error
    public static final String GZS_ERROR = "gzs.error";

    //metric name for new etrace metric
    public static final String GLOBALID_CONFIG_ERROR = "globalid.config.error";
    public static final String GLOBALID_CONFIG_EMPTY = "globalid.config.empty";
    public static final String GLOBALID_SEED_UTIL = "globalid.seed.util";
    public static final String GLOBALID_SEED_VALUE = "globalid.seed.value";

    //dalGroup health check
    public static final String DALGROUP_HEALTH_CHECK = "dalgroup.unhealthy";
    // dalGroup的master ezone状态
    public static final String DALGROUP_MASTER_EZONE = "dalgroup.master.ezone";

    public static final String DALGROUP_CHANGE = "dalgroup.change";

    public static final String DALGROUP_CONFIG_LOAD = "dalgroup.config.load";
    public static final String DALGROUP_CONFIG_WRITE_FAILED = "dalgroup.write.failed";

    public static final String DAL_UPGRADE = "dal.upgrade";

    public static final String ZK_CONNECTION = "curator.connection";

    public static final String ZK_CACHE = "curator.cache";

    // sql with risk may run normal now
    public static final String SQL_RISK_SHARDINGSELECTALL = "sql.risk.shardingselectall";

    public static final String DB_SESSION_WARM_UP = "db.session.warmup";

    public static final String BACKUP_SLAVE_ONLINE = "backup.slave.online";

    // zk heartbeat
    public static final String ZK_HEARTBEAT = "zk.heartbeat";

    public static final String DANGER_SQL_FILTER_DISCARD = "danger.sql.filter.discard";
}
