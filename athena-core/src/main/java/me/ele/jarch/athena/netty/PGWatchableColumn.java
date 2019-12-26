package me.ele.jarch.athena.netty;

import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.pg.proto.PGFlags;
import me.ele.jarch.athena.pg.proto.RowDescription;

import java.util.Objects;

/**
 * Created by zhengchao on 2017/2/17.
 */
public class PGWatchableColumn implements WatchableColumn {
    private RowDescription.PGColumn column = null;
    private String type = "";
    private long threshHold = -1;

    public PGWatchableColumn(RowDescription.PGColumn column, String type, long threshHold) {
        this.column = column;
        this.type = type;
        this.threshHold = threshHold;
    }

    @Override public String getTableName() {
        if (Objects.isNull(column)) {
            return UNKOWN_TABLE;
        }

        return "tableOid_" + this.column.getTableOid();
    }

    @Override public String getColumnName() {
        if (Objects.isNull(column)) {
            return UNKOWN_COLUMN;
        }

        return String.format("columnIndex_%d", this.column.getColNo());
    }

    @Override public String getType() {
        return this.type;
    }

    @Override public long getWatchingThreshHold() {
        return this.threshHold;
    }

    public static WatchableColumn createWatchableColumn(RowDescription.PGColumn column) {
        WatchableColumn watchableColumn = null;
        if (column.getFormat() != 0) {
            // Format 0 is text mode, which is what we really cares about.
            return null;
        }

        if (column.getTypeOid() == PGFlags.INT4_OID) {
            watchableColumn = new PGWatchableColumn(column, COLUMN_TYPE_INT,
                Constants.INT_OVERFLOW_TRACE_THRESHHOLD);
        } else if (column.getTypeOid() == PGFlags.INT2_OID) {
            watchableColumn = new PGWatchableColumn(column, COLUMN_TYPE_SMALL_INT,
                Constants.SMALL_INT_OVERFLOW_TRACE_THRESHHOLD);
        }
        return watchableColumn;
    }
}
