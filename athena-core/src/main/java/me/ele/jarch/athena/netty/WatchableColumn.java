package me.ele.jarch.athena.netty;

/**
 * Created by zhengchao on 2017/2/17.
 */
public interface WatchableColumn {
    public static final String COLUMN_TYPE_INT = "INT";
    public static final String COLUMN_TYPE_SMALL_INT = "SMALL_INT";
    public static final String COLUMN_TYPE_MEDIUM_INT = "MEDIUM_INT";
    public static final String UNKOWN_COLUMN = "unkown_column";
    public static final String UNKOWN_TABLE = "unkown_table";

    public String getTableName();

    public String getColumnName();

    public String getType();

    public long getWatchingThreshHold();
}
