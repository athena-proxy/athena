package me.ele.jarch.athena.sharding;

import me.ele.jarch.athena.constant.EventTypes;
import me.ele.jarch.athena.pg.proto.CommandComplete;
import me.ele.jarch.athena.pg.proto.DataRow;
import me.ele.jarch.athena.pg.proto.ReadyForQuery;
import me.ele.jarch.athena.util.AggregateFunc;
import me.ele.jarch.athena.util.common.ReadOnlyBoolean;
import me.ele.jarch.athena.util.etrace.TraceEnhancer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by jinghao.wang on 17/7/19.
 */
public class PGShardingResultSet extends ShardingResultSet {
    private static final Logger LOG = LoggerFactory.getLogger(PGShardingResultSet.class);

    private final ReadOnlyBoolean rowDescriptionSent = new ReadOnlyBoolean(false);
    private int sentRows;

    public PGShardingResultSet(List<AggregateFunc> columnAggeTypes, boolean isGroupBy) {
        super(columnAggeTypes, isGroupBy);
    }

    @Override public List<byte[]> toPartPackets(boolean withEOF) {
        List<byte[]> packets = new ArrayList<>();
        // 只有没有发送过RowDescription包,且收到过RowDescription包才构造RowDescription包
        // TODO 如果ReadOnlyBoolean使用场景太少,考虑删除此类
        if (columns.size() > 0 && rowDescriptionSent.compareAndSet(false, true)) {
            packets.add(columns.get(0));

        }
        packets.addAll(rows);
        sentRows += rows.size();
        if (withEOF) {
            packets.add(rewriteTag(sentRows).toPacket());
            commandCompletes.clear();
            validateReadyForQueryStatus();
            packets.add(readyForQuerys.get(0));
            readyForQuerys.clear();
        }
        columns.clear();
        rows.clear();
        return packets;
    }

    private CommandComplete rewriteTag(int sentRows) {
        assert commandCompletes.size() > 1;
        CommandComplete commandComplete = CommandComplete.loadFromPacket(commandCompletes.get(0));
        String tagSample = commandComplete.getTag();
        String tagPrefix = tagSample.split(" ")[0];
        commandComplete.setTag(String.format("%s %d", tagPrefix, sentRows));
        return commandComplete;
    }

    private void validateReadyForQueryStatus() {
        Set<Byte> statuses = new HashSet<>();
        for (byte[] readyForQuery : readyForQuerys) {
            ReadyForQuery rfq = ReadyForQuery.loadFromPacket(readyForQuery);
            statuses.add(rfq.getStatus());
        }
        if (statuses.size() != 1) {
            throw new RuntimeException(String.format(
                "inconsistent readyForQuery Status %s when validate PostgreSQL shard response",
                statuses));
        }
    }

    @Override protected void aggregateRows(List<byte[]> newRows) {
        // only support no group aggregation
        if (rows.size() != 1) {
            // TODO 此语句块永远不会进入,暂时标记，以后择机删除
            LOG.error("Only support for no group aggregation");
            rows.addAll(newRows);
            return;
        }
        List<byte[]> rowList = new ArrayList<>();
        rowList.add(rows.get(0));
        rowList.addAll(newRows);
        AggregateObject[] columns = new AggregateObject[columnAggeTypes.size()];
        int rowSize = rowList.size();
        for (int j = 0; j < rowSize; j++) {
            byte[] rowBytes = rowList.get(j);
            DataRow row = DataRow.loadFromPacket(rowBytes);
            for (int i = 0; i < columnAggeTypes.size(); i++) {
                try {
                    switch (columnAggeTypes.get(i)) {
                        case COUNT:
                        case SUM:
                        case MAX:
                        case MIN:
                            // 聚合函数的个数(包含NONE)必然==返回列的个数,此处无需担心数组越界问题
                            DataRow.PGCol col = row.getCols()[i];
                            // data为null,意味着结果为NULL.
                            // 在聚合函数的场景中不会出现,所以`col.getData()`永远不会为null
                            String strVal = new String(col.getData(), StandardCharsets.UTF_8);
                            AggregateObject colValue =
                                new AggregateObject(strVal, new BigDecimal(strVal));
                            if (columns[i] == null) {
                                columns[i] = colValue;
                                break;
                            }
                            columns[i] = columnAggeTypes.get(i).aggregate(columns[i], colValue);
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
        DataRow rowFirst = DataRow.loadFromPacket(rowList.get(0));
        DataRow.PGCol[] pgCols = new DataRow.PGCol[rowFirst.getColCount()];
        for (int i = 0; i < columnAggeTypes.size(); i++) {
            switch (columnAggeTypes.get(i)) {
                case COUNT:
                case SUM:
                case MAX:
                case MIN:
                    pgCols[i] =
                        new DataRow.PGCol(columns[i].getStrVal().getBytes(StandardCharsets.UTF_8));
                    break;
                case NONE:
                default:
                    pgCols[i] = rowFirst.getCols()[i];
                    break;
            }
        }
        rows.clear();
        rows.add(new DataRow(pgCols).toPacket());
    }
}
