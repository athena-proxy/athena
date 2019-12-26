package me.ele.jarch.athena.constant;

import java.lang.management.ManagementFactory;

public class Constants {
    public static final String APPID = System.getProperty("APPID", "Unkown");
    public static final String HOSTNAME = System.getProperty("HOSTNAME", "Unkown");
    public static final String SHORT_APPID;
    public static final int TYPICAL_SQL_SIZE = 4096;
    public static final int TYPICAL_COMMENT_SIZE = 256;

    static {
        int idx = APPID.lastIndexOf('.');
        if (idx != -1) {
            SHORT_APPID = APPID.substring(idx + 1);
        } else {
            SHORT_APPID = APPID;
        }
    }

    public static final String FULL_HOSTNAME = System.getenv("FULL_HOSTNAME");
    public static final String ENV =
        System.getenv("ELEME_ENV") != null ? System.getenv("ELEME_ENV") : "dev";
    public static final String DAL_SEQUENCES_USER = "$dal_sequences_user";
    public static final String SLAVE_OFFLINE_FLAGS = "slave_offline_flags";
    public static final String FUSE_DB_FLAGS = "fuse_db_flags";
    public static final String FUSE_PASS_RATE = "fuse_pass_rate";
    public static final String FUSE_WINDOW_MILLIS = "fuse_window_millis";
    public static final String SMOKE_WINDOW_MILLIS = "smoke_window_millis";
    public static final String ELEME_ORDER_DOUBLE_WRITE = "eleme_order_double_write";
    public static final String PID;
    public static final String MEDIATOR = "mediator";
    public static final String DALGROUP_CFG_FOLDER = "/data/run/" + APPID + "/dalgroup_cache";

    static {
        String pid = ManagementFactory.getRuntimeMXBean().getName();
        if (pid != null && pid.indexOf("@") != -1) {
            pid = pid.substring(0, pid.indexOf("@"));
        }
        PID = pid;
    }

    public static final String ROLE = System.getProperty("role", "normal");

    public static final int FIXED_DEBUG_PORT = 8844;
    public static final int MIN_DYNAMIC_DEBUG_PORT = 8845;
    public static final int MAX_DYNAMIC_DEBUG_PORT = 8899;
    public static final int DUMMY_SEMAPHORE_MAX_COUNT = 5000;
    public static final int SEND_CLIENT_MAX_BUFFER = 64 * 1024;
    public static final int MAX_PG_SERVICE_PORT = 7999;
    public static final int MIN_MYSQL_SERVICE_PORT = 9000;
    public static final String MAX_RESULT_BUF_SIZE = "max_result_buf_size";
    public static final String MAX_ACTIVE_DB_SESSIONS = "max_active_db_sessions";
    public static final String MAX_ACTIVE_DB_TRANS = "max_active_db_trans";
    public static final String MAX_QUEUE_SIZE = "max_queue_size";
    public static final String REJECT_SQL_BY_PATTERN = "reject_sql_by_pattern";
    public static final String REJECT_SQL_BY_REGULAR_EXP = "reject_sql_by_regularexp";
    public static final String REJECT_SQL_BY_REAL_REGULAR_EXP = "reject_sql_by_real_regularexp";
    public static final String BIND_MASTER_PERIOD = "bind_master_period";
    public static final String BIND_MASTER_SQLS = "bind_master_sqls";
    public static final String MASTER_HEARTBEAT_FASTFAIL_PERIOD =
        "master_heartbeat_fastfail_period";
    public static final String OVERFLOW_TRACE_FREQUENCY = "overflow_trace_frequency";
    public static final long SMALL_INT_OVERFLOW_TRACE_THRESHHOLD = 16383;
    public static final long MEDIUM_INT_OVERFLOW_TRACE_THRESHHOLD = 4194303;
    /**
     * 阈值对应的时间戳时间为'2022/12/30 11:00:00', 占int最大值的77%左右
     * 设为此值是为了避免有用户用int字段存储时间戳，产生大量告警噪音
     */
    public static final long INT_OVERFLOW_TRACE_THRESHHOLD = 1672369200;
    public static final String TRAFFIC_POLICINGS = "traffic_policings";
    public static final String CLIENT_LOGIN_TIMEOUT_IN_MILLS = "client_login_timeout_in_mills";
    public static final String CLIENT_LOGIN_SLOW_IN_MILLS = "client_login_slow_in_mills";
    public static final String APPEND_INDEX_HINTS = "append_index_hints";
    public static final String PURE_SLAVE_ONLY = "pure_slave_only";

    // AutoKiller
    public static final String AK_SLOW_SQL = "autokill_slow_sql";
    public static final String AK_SLOW_COMMIT = "autokill_slow_commit";
    public static final String AK_OPEN = "autokill_open";
    public static final String AK_LOWER_SIZE = "autokill_lower_size";
    public static final String AK_UPPER_SIZE = "autokill_upper_size";
    public static final String SMART_AK_OPEN = "smart_autokill";

    // delay return error
    public static final String DELAY_RETURN_ERROR = "delay_return_error";

    // zk configured temp white fields
    public static final String WHITE_FIELDS = "white_fields";
    public static final String WHITE_FIELDS_DEADLINE = "white_fields_deadline";

    // Err Message
    public static final String DAL = "[DAL]";
    public static final String OVER_MAX_RESULT_BUF_SIZE = "Max resultset size.";
    public static final String NOT_SUPPORT_FUNCTION = "Not supported functions: '%s'";
    public static final String MULTIPLE_KEYS_DURING_TRANSACTION =
        "Multiple sharding keys during transaction";
    public static final String CHAOS_QUERYS_DURING_TRANSACTION =
        "Mix sharding and non sharding querys during transaction";
    public static final String INVALID_SHARDING_SQL = "Invalid sharding SQL: %s , errorMessage: %s";
    public static final String INVALID_SQL_SYNTAX = "Invalid SQL Syntax: %s , errorMessage: %s";

    // public static final long PROTOCOL_VERSION = 10;
    public static final String SERVER_VERSION = "5.6.21-log";
    // public static final String SERVER_VERSION = "5.6.0-athena-0.0.0";
    public static final long MAX_PACKET_SIZE = (long) 1024 * 1024 * 16;

    // other
    public static final String DAL_DEBUG_SERVER_FILE =
        "/tmp/dal_debug_server_" + SHORT_APPID + "-" + PID + ".txt";

    // netty autoread 触发大小 10485760 (64K)
    public static final int MAX_AUTOREAD_TRIGGER_SIZE = 64 * 1024;

    // etrace
    public static final String ETRACE_SAMPLE_RATE = "etrace_sample_rate";
    // heartbeat
    public static final String HEARTBEAT = "heart_beat";
    // weak master
    public static final String WEAK_MASTER_SWITCH = "weak_master_switch";

    // transId trace
    public static final String QUERY_PREFIX = "q";
    public static final String TRANS_PREFIX = "t";
    public static final String AUTOCOMMIT_PREFIX = "a";
    public static final String DEFAULT_TRANS_ID = QUERY_PREFIX + 0;

    // userInfo attributes
    public static final String READ_ONLY = "readonly";

    public static final int STARTUP_DELAY_TIME = 20000;

    // smooth upgrade
    public static final String UPGRADE_AUDS_PATH =
        String.format("\0/data/run/dal_upgrade/%s_upgrade_auds.sock", APPID);
    public static final String DOWNGRADE_AUDS_PATH =
        String.format("\0/data/run/dal_upgrade/%s_downgrade_auds.sock", APPID);
    public static final int DYING_TIME_LIMIT = 6 * 60 * 1000;

    // dal config field
    public static final String DAL_SEQUENCE_TABLE = "dal_sequence_table";
    public static final String DAL_SEQUENCE_ENV = "dal_sequence_env";
    public static final String DAL_SEQUENCE_CACHE_POLL = "dal_sequence_cache_pool";
    /**
     * admin username
     */
    public static final String ATHENA_ADMIN = "athena_admin";
    /**
     * readonly admin username
     */
    public static final String ATHENA_ADMIN_READONLY = "athena_admin_readonly";
    // goproxy heartbeat username
    public static final String DAL_HEARTBEAT = "dal_heartbeat";

    public static final String MANUAL_RELEASE_DEFAULT = "yes";
    public static final String MANUAL_RELEASE =
        System.getProperty("manual_release", MANUAL_RELEASE_DEFAULT).toLowerCase();
    public static final String REUSEPORT_SUPPORT_DEFAULT = "yes";
    public static final String REUSEPORT_SUPPORT =
        System.getProperty("reuseport_support", REUSEPORT_SUPPORT_DEFAULT).toLowerCase();
    public static final String UPGRADE_FLAG_FILE_PREFIX = APPID + "_athena_smooth_upgrade_flag_";
    public static final long UPGRADE_TIME_LIMIT = (long) 5 * 60 * 1000;

    // shadow db
    public static final String DAL_SHADOW_DB = "dal_shadow_db";
    public static final String ALLOW_SHADOWDB = "allow_shadowdb";

    // close service ports delay when in smooth down mode
    public static final long SMOOTH_DOWN_DELAY_IN_MILLS = 10 * 1000;

    // pg db
    public static final String DB_VENDOR = "db_vendor";
    public static final String DIST_MASTER = "dist_master";
    public static final String PG_DB_OVERDUE_IN_MINUTES = "pg_db_overdue_in_minutes";
    public static final String PG_SERVER_VERSION = "9.4.8";

    public static final String BATCH_SWITCH_NAME = "allow_batch_switch";
    public static final String LOCAL_CHANNEL_SERVER_ADDRESS = "athena_local_channel_address";
    public static final String ELE_META_BATCH_ANALYZED_MARKER = "batch";
    public static final String ELE_META_DAL_GROUP = "DALGroup";
    public static final String ELE_META_AUTO_COMMIT_FOR_CLIENT = "autoCommit4Client";
    public static final String ELE_META_BIND_MASTER = "bindM";
    public static final String ELE_META_TRANSID = "transId";
    public static final String ELE_META_SHARDING_INDEX = "shardingidx";
    public static final String ELE_META_SHARD_COL_VAL = "shardColVal";

    public static final String EZONE_SHARD_INFO = "ezone_shard_info";

    // globalid partition feature switch
    public static final String GLOBALID_PARTITION_SWITCH = "globalid_partition_switch";

    // for detect slow dynamic load jar switch
    public static final String DETECT_SLOW_SWITCH = "detect_slow_switch";

    public static final int REWIRTE_IN_CONDITION_THRESHOLD = 100;

    // send sql to rmq switch
    public static final String SEND_AUDIT_SQL_TO_RMQ_SWITCH = "send_audit_sql_to_rmq_switch";
    // send sql to eagle rmq threshold
    public static final String SEND_AUDIT_SQL_TO_RMQ_THRESHOLD = "send_audit_sql_to_rmq_threshold";

    public static final String CREATED_AT = "created_at";

    public static final String DAL_CONFIG_ERR_PWD = "DAL_CONFIG_ERR_PWD";

    // Login trace phrase
    public static final String CHANNEL_ACTIVE = "channelActive";
    public static final String MYSQL_HANDSHAKE_SENT = "handshakeSent";
    public static final String MYSQL_HANDSHAKE_RESPONSE_RECEIVED = "respRecv";
    public static final String AUTH_OK_SENT = "authOkSent";
    public static final String AUTH_OK_BROKEN = "authOkBroken";
    public static final String AUTH_ERR = "authErr";
    public static final String AUTH_TIMEOUT = "authTimeout";

    public static final String PG_SSL_RECEIVED = "SSLRecv";
    public static final String PG_STARTUP_RECEIVED = "startupRecv";
    public static final String PG_AUTH_RESPONSE_RECEIVED = "authPwdRecv";
    public static final String PG_NO_SSL_SENT = "noSSLSent";
    public static final String PG_AUTH_METHOD_SENT = "authMethodSent";

    // whether send new etrace metric
    public static final String SEND_ETRACE_METRIC_SWITCH = "send_etrace_metric_switch";

    public static final String DAL_GROUP_HEALTH_CHECK_SWITCH = "dal_group_health_check_switch";

    public static final String BIND_MASTER = "bind_master";

    public static final String MHA_SLAVE_FAIL_SWITCH = "mha_slave_fail_switch";

    public static final String MAX_SQL_PATTERN_LENGTH = "max_sql_pattern_length";

    public static final String UNKNOWN = "unknown";
    public static final String EZONE = "ezone";

    public static final String DALGROUP_CFG_SWITCH = "dalgroup_cfg_switch";
    public static final String OLD_CONFIGFILE_DISABLED = "old_configfile_switch";
    public static final String ZK_DALGROUP_WATCHER = "zk_dalgroup_watcher";

    public static final String DUMMY = "DUMMY";
    // extend db role, only have `primary` now
    public static final String EX_ROLE = "ex_role";
    public static final String PRIMARY = "primary";

    public static final String SLAVE_SELECT_STRATEGY = "slave_select_strategy";
    // strategy name
    public static final String PRIMARY_BACKUP = "primary_backup";

    public static final String CONFIG_VERSION = "config_version";

    public static final String MAX_SQL_LENGTH = "max_sql_length";

    public static final String MAX_RESPONSE_LENGTH = "max_response_length";

    public static final String QUERY_SQL_LOG_SWITCH = "query_sql_log_switch";

    public static final String HEALTCH_CHECK_INTERVAL = "health_check_interval";

    public static final String WARM_UP_SESSION_COUNT = "warm_up_session_count";

    public static final String SHARDING_HASH_CHECK_LEVEL = "sharding_hash_check_level";

    public static final String IS_SPECIFY_DB = "is_specify_db";

    public static final String DANGER_SQL_FILTER_ENABLED = "danger_sql_filter_enabled";
}
