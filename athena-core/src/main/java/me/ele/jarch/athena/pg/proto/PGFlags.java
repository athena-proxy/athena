package me.ele.jarch.athena.pg.proto;

/**
 * Created by jinghao.wang on 16/11/23.
 */
public class PGFlags {
    // common
    public static final byte B_COPY_DATA = (byte) 'd';
    public static final byte B_COPY_DONE = (byte) 'c';

    // DAL自定义的预处理类型(也属于client side)
    public static final byte C_CANCEL_REQUEST = -1;
    public static final byte C_SSL_REQUEST = -2;
    public static final byte C_STARTUP_MESSAGE = -3;
    public static final byte C_SSL_NOT_SUPPOTED = -4;

    // client side
    public static final byte C_BIND = (byte) 'B';
    public static final byte C_CLOSE = (byte) 'C';
    public static final byte C_COPY_FAIL = (byte) 'f';
    public static final byte C_DESCRIBE = (byte) 'D';
    public static final byte C_EXECUTE = (byte) 'E';
    public static final byte C_FLUSH = (byte) 'H';
    public static final byte C_FUNCTION_CALL = (byte) 'F';
    public static final byte C_PARSE = (byte) 'P';
    public static final byte C_PASSWORD_MESSAGE = (byte) 'p';
    public static final byte C_QUERY = (byte) 'Q';
    public static final byte C_SYNC = (byte) 'S';
    public static final byte C_TERMINATE = (byte) 'X';
    // server side
    public static final byte AUTHENTICATION_OK = (byte) 'R';
    public static final byte AUTHENTICATION_KERBEROS_V5 = (byte) 'R';
    public static final byte AUTHENTICATION_CLEARTEXT_PASSWORD = (byte) 'R';
    public static final byte AUTHENTICATION_MD5_PASSWORD = (byte) 'R';
    public static final byte AUTHENTICATION_SCM_CREDENTIAL = (byte) 'R';
    public static final byte AUTHENTICATION_GSS = (byte) 'R';
    public static final byte AUTHENTICATION_SSPI = (byte) 'R';
    public static final byte AUTHENTICATION_GSS_CONTINUE = (byte) 'R';
    public static final byte BACKEND_KEY_DATA = (byte) 'K';
    public static final byte BIND_COMPLETE = (byte) '2';
    public static final byte CLOSE_COMPLETE = (byte) '3';
    public static final byte COMMAND_COMPLETE = (byte) 'C';
    public static final byte COPY_IN_RESPONSE = (byte) 'G';
    public static final byte COPY_OUT_RESPONSE = (byte) 'H';
    public static final byte COPY_BOTH_RESPONSE = (byte) 'W';
    public static final byte DATA_ROW = (byte) 'D';
    public static final byte EMPTY_QUERY_RESPONSE = (byte) 'I';
    public static final byte ERROR_RESPONSE = (byte) 'E';
    public static final byte FUNCTION_CALL_RESPONSE = (byte) 'V';
    public static final byte NO_DATA = (byte) 'n';
    public static final byte NOTICE_RESPONSE = (byte) 'N';
    public static final byte NOTIFICATION_RESPONSE = (byte) 'A';
    public static final byte PARAMETER_DESCRIPTION = (byte) 't';
    public static final byte PARAMETER_STATUS = (byte) 'S';
    public static final byte PARSE_COMPLETE = (byte) '1';
    public static final byte PORTAL_SUSPENDED = (byte) 's';
    public static final byte READY_FOR_QUERY = (byte) 'Z';
    public static final byte ROW_DESCRIPTION = (byte) 'T';

    //single byte token, appear in ErrorResponse and NoticeResponse messages
    public static final byte SEVERITY = (byte) 'S';
    public static final byte SEVERITY_V9_6 = (byte) 'V';
    public static final byte CODE = (byte) 'C';
    public static final byte MESSAGE = (byte) 'M';
    public static final byte DETAIL = (byte) 'D';
    public static final byte HINT = (byte) 'H';
    public static final byte POSITION = (byte) 'P';
    public static final byte INTERNAL_POSITION = (byte) 'p';
    public static final byte INTERNAL_QUERY = (byte) 'q';
    public static final byte WHERE = (byte) 'W';
    public static final byte SCHEMA = (byte) 's';
    public static final byte TABLE = (byte) 't';
    public static final byte COLUMN = (byte) 'c';
    public static final byte DATA_TYPE = (byte) 'd';
    public static final byte CONSTRAINT = (byte) 'n';
    public static final byte FILE = (byte) 'F';
    public static final byte LINE = (byte) 'L';
    public static final byte ROUTINE = (byte) 'R';

    // for data type oids
    public static final int INT8_OID = 20;
    public static final int INT2_OID = 21;
    public static final int INT4_OID = 23;
    public static final int FLOAT4_OID = 700;
    public static final int FLOAT8_OID = 701;
    public static final int MONEY_OID = 790;
    public static final int NUMERIC_OID = 1700;
}
