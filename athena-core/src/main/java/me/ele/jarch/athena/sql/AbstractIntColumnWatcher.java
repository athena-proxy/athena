package me.ele.jarch.athena.sql;

import io.etrace.common.Constants;
import me.ele.jarch.athena.allinone.DBVendor;
import me.ele.jarch.athena.constant.EventTypes;
import me.ele.jarch.athena.constant.TraceNames;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.netty.WatchableColumn;
import me.ele.jarch.athena.util.GreySwitch;
import me.ele.jarch.athena.util.etrace.DalMultiMessageProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by zhengchao on 2017/2/27.
 */
public abstract class AbstractIntColumnWatcher {
    private static final Logger logger = LoggerFactory.getLogger(AbstractIntColumnWatcher.class);
    private static final long INT_OVERFLOW_RANDOM_BASE = 10000;
    protected SqlSessionContext sqlCtx = null;
    protected Map<Integer, WatchableColumn> watchingColumns = new HashMap<>();
    protected int columnIndexForWatching = 0;
    private int affectedRows = 0;

    public static AbstractIntColumnWatcher intColumnWatcherFactory(DBVendor vendor) {
        long bound = GreySwitch.getInstance().getOverflowTraceFrequency();
        long random = ThreadLocalRandom.current().nextLong(INT_OVERFLOW_RANDOM_BASE);
        if (random > bound) {
            return EmptyIntColumnWatcher.EMPTY;
        }

        switch (vendor) {
            case MYSQL:
                return new MysqlIntColumnWatcher();
            case PG:
                return new PGIntColumnWatcher();
            default:
                return EmptyIntColumnWatcher.EMPTY;
        }
    }

    public void startIntColumnAnalyze(final SqlSessionContext sqlCtx, final byte[] cmdColPacket) {
        if (Objects.isNull(sqlCtx)) {
            return;
        }

        try {
            this.sqlCtx = sqlCtx;
            addWatchingColumnIfNecessary(cmdColPacket);
        } catch (Throwable e) {
            logger.error("error in int column extraction", e);
        }
    }

    public void fireTraceIfNecessary(final byte[] packet) {
        if (Objects.isNull(sqlCtx)) {
            return;
        }
        if (affectedRows++ != 0) {
            return;
        }

        try {
            List<String> columns = extractContent(packet);
            fireTrace(columns);
        } catch (Throwable e) {
            logger.error("error when fire trace", e);
        }
    }

    private void fireTrace(List<String> columnData) {
        if (watchingColumns.isEmpty()) {
            return;
        }

        int columnIndex = 0;
        for (String cd : columnData) {
            WatchableColumn column = watchingColumns.getOrDefault(columnIndex, null);
            ++columnIndex;
            if (Objects.isNull(column) || Objects.isNull(cd)) {
                continue;
            }
            if (Long.parseLong(cd) > column.getWatchingThreshHold()) {
                fireEtraceForWatchableColumn(column, cd);
            }
        }
    }

    private void fireEtraceForWatchableColumn(WatchableColumn column, String columnData) {
        DalMultiMessageProducer producer = sqlCtx.sqlSessionContextUtil.getDalEtraceProducer();
        String dalGroupName = sqlCtx.getHolder().getDalGroup().getName();
        String tableName = column.getTableName();
        String columnName = column.getColumnName();
        String columnType = column.getType();
        String threshold = String.valueOf(column.getWatchingThreshHold());
        producer.newFluentEvent(EventTypes.RISK_FIELD_OVERFLOW, dalGroupName)
            .tag(TraceNames.TABLE, tableName).tag("columnName", columnName)
            .tag("columnType", columnType).tag("threshold", threshold).data("value:" + columnData)
            .status(Constants.SUCCESS).complete();
    }

    protected abstract List<String> extractContent(final byte[] packet);

    protected abstract void addWatchingColumnIfNecessary(final byte[] cmdColPacket);
}
