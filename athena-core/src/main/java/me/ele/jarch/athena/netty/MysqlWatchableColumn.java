package me.ele.jarch.athena.netty;

import com.github.mpjct.jmpjct.mysql.proto.Column;
import com.github.mpjct.jmpjct.mysql.proto.Flags;
import me.ele.jarch.athena.constant.Constants;

import java.util.Objects;

/**
 * Created by zhengchao on 2017/2/17.
 */
public class MysqlWatchableColumn implements WatchableColumn {
    private Column column = null;
    private String type = "";
    private long threshHold = -1;

    public MysqlWatchableColumn(Column column, String type, long threshHold) {
        this.column = column;
        this.type = type;
        this.threshHold = threshHold;
    }

    @Override public String getTableName() {
        if (Objects.isNull(this.column)) {
            return UNKOWN_TABLE;
        }

        if (this.column.org_table.equals("*")) {
            /*
             * This is for sub-query in select:
             * `select b.money from (select id as money from sharding_none) as b where b.money < 3;`
             *
             * In the above query, the sub-query is aliased as b, which is a non-existing table.
             * But in the packet returned by MySQL, the original table name is `*`, which might cause unexpected behavior
             * in etrace or metrics. So we replace it with a common string here.
             *
             * Notice that the original column name for b is `money`, which is the column name used in sub-query,
             * easier for negotiating with users.
             * */
            return UNKOWN_TABLE;
        }

        return this.column.org_table;
    }

    @Override public String getColumnName() {
        if (Objects.isNull(this.column)) {
            return UNKOWN_COLUMN;
        }

        return this.column.org_name;
    }

    @Override public String getType() {
        return this.type;
    }

    @Override public long getWatchingThreshHold() {
        // unsigned type return double of threshold
        return column.hasFlag(Flags.UNSIGNED_FLAG) ? threshHold * 2 : threshHold;
    }

    public static WatchableColumn createWatchableColumn(Column column) {
        WatchableColumn watchableColumn = null;
        if (column.type == Flags.MYSQL_TYPE_LONG) {
            watchableColumn = new MysqlWatchableColumn(column, COLUMN_TYPE_INT,
                Constants.INT_OVERFLOW_TRACE_THRESHHOLD);
        } else if (column.type == Flags.MYSQL_TYPE_SHORT) {
            watchableColumn = new MysqlWatchableColumn(column, COLUMN_TYPE_SMALL_INT,
                Constants.SMALL_INT_OVERFLOW_TRACE_THRESHHOLD);
        } else if (column.type == Flags.MYSQL_TYPE_INT24) {
            watchableColumn = new MysqlWatchableColumn(column, COLUMN_TYPE_MEDIUM_INT,
                Constants.MEDIUM_INT_OVERFLOW_TRACE_THRESHHOLD);
        }

        return watchableColumn;
    }

}
