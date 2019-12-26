package me.ele.jarch.athena.sharding;

import com.github.mpjct.jmpjct.mysql.proto.EOF;
import com.github.mpjct.jmpjct.mysql.proto.*;
import me.ele.jarch.athena.constant.EventTypes;
import me.ele.jarch.athena.util.AggregateFunc;
import me.ele.jarch.athena.util.etrace.TraceEnhancer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.github.mpjct.jmpjct.mysql.proto.Flags.*;

/**
 * Created by jinghao.wang on 17/7/19.
 */
public class MysqlShardingResultSet extends ShardingResultSet {
    private static final Logger LOG = LoggerFactory.getLogger(MysqlShardingResultSet.class);
    private List<Long> aggreColumnType = new ArrayList<>();

    private static final DateTimeFormatter DATE_TIME_FORMATTER =
        new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd HH:mm:ss")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 6, true).toFormatter();

    private static final DateTimeFormatter TIME_FORMATTER =
        new DateTimeFormatterBuilder().appendPattern("HH:mm:ss")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 6, true).toFormatter();

    public MysqlShardingResultSet(List<AggregateFunc> columnAggeTypes, boolean isGroupBy) {
        super(columnAggeTypes, isGroupBy);
    }

    @Override public void addCommandComplete(byte[] commandComplete) {
        throw new RuntimeException("Called addCommandComplete in MysqlShardingResultSet");
    }

    @Override public void addReadyForQuery(byte[] readForQuery) {
        throw new RuntimeException("Called addReadyForQuery in MysqlShardingResultSet");
    }

    @Override public List<byte[]> toPartPackets(boolean withEOF) {
        ArrayList<byte[]> packets = new ArrayList<byte[]>();
        EOF eof;
        if (this.columns.size() > 0) {
            ColCount colCount = new ColCount();
            colCount.sequenceId = this.sequenceId;
            this.sequenceId++;
            colCount.colCount = this.columns.size();
            packets.add(colCount.toPacket());

            for (byte[] col : this.columns) {
                System.arraycopy(Proto.build_fixed_int(1, this.sequenceId), 0, col, 3, 1);
                this.sequenceId++;
                packets.add(col);
            }
            this.columns.clear();

            eof = new EOF();
            eof.sequenceId = this.sequenceId;
            this.sequenceId++;
            packets.add(eof.toPacket());
        }

        for (byte[] row : this.rows) {
            System.arraycopy(Proto.build_fixed_int(1, this.sequenceId), 0, row, 3, 1);
            this.sequenceId++;
            packets.add(row);
        }
        this.rows.clear();

        if (withEOF) {
            eof = new EOF();
            eof.sequenceId = this.sequenceId;
            this.sequenceId++;
            packets.add(eof.toPacket());
        }

        return packets;
    }

    @Override protected void aggregateRows(List<byte[]> newRows) {
        // only support no group aggregation
        if (this.rows.size() != 1) {
            // TODO 此语句块永远不会进入,暂时标记，以后择机删除
            LOG.error("Only support for no group aggregation");
            this.rows.addAll(newRows);
            return;
        }
        List<byte[]> rowList = new ArrayList<>();
        rowList.add(this.rows.get(0));
        rowList.addAll(newRows);
        AggregateObject[] columns = new AggregateObject[columnAggeTypes.size()];
        int rowSize = rowList.size();
        for (int j = 0; j < rowSize; j++) {
            byte[] rowBytes = rowList.get(j);
            Row row = Row.loadFromPacket(rowBytes, this.columns.size());

            for (int i = 0; i < columnAggeTypes.size(); i++) {
                try {
                    switch (columnAggeTypes.get(i)) {
                        case COUNT:
                        case SUM:
                        case MAX:
                        case MIN:
                            if (row.data.get(i) == null) {
                                break;
                            }

                            String strVal = row.data.get(i).toString();
                            Comparable comparableVal;
                            long columnType = getColumnTypeByIndex(i);
                            if (isDateTimeColumn(columnType)) {
                                String value = row.data.get(i).toString();
                                comparableVal = getDateTimeByType(value, (int) columnType);
                            } else {
                                comparableVal = new BigDecimal(strVal);
                            }
                            AggregateObject aggregateObject =
                                new AggregateObject(strVal, comparableVal);

                            if (columns[i] == null) {
                                columns[i] = aggregateObject;
                                break;
                            }
                            columns[i] =
                                columnAggeTypes.get(i).aggregate(columns[i], aggregateObject);
                            break;
                        case NONE:
                        default:
                            break;
                    }
                } catch (NumberFormatException e) {
                    LOG.warn("Error convert to Number.Row=" + row, e);
                    TraceEnhancer.newFluentEvent(EventTypes.ILLEGAL_AGGREGATE_COLUMN, "illegal")
                        .data("row: " + row).status("illegal").complete();
                } catch (Exception e) {
                    LOG.debug("Error null for Row.data.Row=" + row, e);
                    TraceEnhancer.newFluentEvent(EventTypes.ILLEGAL_AGGREGATE_COLUMN, "error")
                        .data("row: " + row).status("unknown_error").complete();
                }
            }
        }

        Row aggregatedRow = new Row();
        Row rowFirst = Row.loadFromPacket(rowList.get(0), this.columns.size());
        for (int i = 0; i < columnAggeTypes.size(); i++) {
            switch (columnAggeTypes.get(i)) {
                case COUNT:
                case SUM:
                case MAX:
                case MIN:
                    String val = Objects.nonNull(columns[i]) ? columns[i].getStrVal() : null;
                    aggregatedRow.addData(val);
                    break;
                case NONE:
                default:
                    aggregatedRow.addData(String.valueOf(rowFirst.data.get(i)));
                    break;
            }
        }
        this.rows.clear();
        this.rows.add(aggregatedRow.toPacket());
    }

    /**
     * by default MySQL retrieves and displays
     * 1. DATETIME/TIMESTAMP values in 'YYYY-MM-DD HH:MM:SS' or 'YYYY-MM-DD HH:MM:SS[.fraction] format.
     * 2. TIME in 'HH:MM:SS' or 'HH:MM:SS[.fraction]' format. we don't support large hours in format 'HHH:MM:SS'
     * 3. YEAR in 'YYYY' or 'YY'
     * this function only supports above format with type respectively
     *
     * @param dt
     * @return
     */
    private LocalDateTime getDateTimeByType(String dt, int type) {
        switch (type) {
            case MYSQL_TYPE_TIMESTAMP:
            case MYSQL_TYPE_DATETIME:
                return LocalDateTime.parse(dt, DATE_TIME_FORMATTER);
            case MYSQL_TYPE_DATE:
            case MYSQL_TYPE_NEWDATE:
                LocalDate date = LocalDate.parse(dt);
                return LocalDateTime.of(date, LocalTime.MIN);
            case MYSQL_TYPE_TIME:
                LocalTime localTime = LocalTime.parse(dt, TIME_FORMATTER);
                return LocalDateTime.of(LocalDate.MIN, localTime);
            case MYSQL_TYPE_YEAR:
                return LocalDateTime.of(LocalDate.of(Integer.parseInt(dt), 1, 1), LocalTime.MIN);
            default:
                return LocalDateTime.MIN;
        }
    }

    private long getColumnTypeByIndex(int index) {
        if (aggreColumnType.size() > index) {
            return aggreColumnType.get(index);
        }
        long type = Column.loadFromPacket(columns.get(index)).type;
        aggreColumnType.add(index, type);
        return type;
    }

    private boolean isDateTimeColumn(long type) {
        return type == MYSQL_TYPE_TIMESTAMP || type == MYSQL_TYPE_DATE || type == MYSQL_TYPE_TIME
            || type == MYSQL_TYPE_NEWDATE || type == MYSQL_TYPE_DATETIME || type == MYSQL_TYPE_YEAR;
    }
}
