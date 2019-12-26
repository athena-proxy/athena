package com.github.mpjct.jmpjct.util;

import com.github.mpjct.jmpjct.mysql.proto.Flags;

public class PacketUtil {
    public static int getServerCapabilities() {
        int flag = 0;
        flag |= Flags.CLIENT_LONG_PASSWORD;
        flag |= Flags.CLIENT_FOUND_ROWS;
        flag |= Flags.CLIENT_LONG_FLAG;
        flag |= Flags.CLIENT_CONNECT_WITH_DB;
        flag |= Flags.CLIENT_NO_SCHEMA;
        flag |= Flags.CLIENT_COMPRESS;
        flag |= Flags.CLIENT_ODBC;
        flag |= Flags.CLIENT_LOCAL_FILES;
        flag |= Flags.CLIENT_IGNORE_SPACE;
        flag |= Flags.CLIENT_PROTOCOL_41;
        flag |= Flags.CLIENT_INTERACTIVE;
        // flag |= Flags.CLIENT_SSL;
        flag |= Flags.CLIENT_IGNORE_SIGPIPE;
        flag |= Flags.CLIENT_TRANSACTIONS;
        flag |= Flags.CLIENT_RESERVED;
        flag |= Flags.CLIENT_SECURE_CONNECTION;
        flag |= Flags.CLIENT_MULTI_RESULTS;
        flag |= Flags.CLIENT_PS_MULTI_RESULTS;
        flag |= Flags.CLIENT_PLUGIN_AUTH;
        flag |= Flags.CLIENT_CONNECT_ATTRS;
        flag |= Flags.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA;
        flag |= Flags.CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS;
        flag |= Flags.CLIENT_SSL_VERIFY_SERVER_CERT;
        flag |= Flags.CLIENT_REMEMBER_OPTIONS;
        return flag;
    }

    /**
     * 与MySQL连接时的一些特性指定
     */
    public static long getClientFlags() {
        int flag = 0;
        flag |= Flags.CLIENT_LONG_PASSWORD;
        flag |= Flags.CLIENT_FOUND_ROWS;
        flag |= Flags.CLIENT_LONG_FLAG;
        flag |= Flags.CLIENT_CONNECT_WITH_DB;
        // flag |= Flags.CLIENT_NO_SCHEMA;
        // flag |= Flags.CLIENT_COMPRESS;
        flag |= Flags.CLIENT_ODBC;
        // flag |= Flags.CLIENT_LOCAL_FILES;
        flag |= Flags.CLIENT_IGNORE_SPACE;
        flag |= Flags.CLIENT_PROTOCOL_41;
        flag |= Flags.CLIENT_INTERACTIVE;
        // flag |= Flags.CLIENT_SSL;
        flag |= Flags.CLIENT_IGNORE_SIGPIPE;
        flag |= Flags.CLIENT_TRANSACTIONS;
        // flag |= Flags.CLIENT_RESERVED;
        flag |= Flags.CLIENT_SECURE_CONNECTION;
        // client extension
        // support for multi query,2016.4.15
        flag |= Flags.CLIENT_MULTI_STATEMENTS;
        flag |= Flags.CLIENT_MULTI_RESULTS;
        return flag;
    }
}
