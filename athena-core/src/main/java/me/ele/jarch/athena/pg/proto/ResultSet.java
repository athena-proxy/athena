package me.ele.jarch.athena.pg.proto;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

/**
 * Note: Only support SELECT query result set, Not Support SHOW, EXPLAIN query result set
 * Created by jinghao.wang on 2017/12/14.
 */
public class ResultSet {
    private RowDescription rowDescription;
    private final List<DataRow> dataRows = new LinkedList<>();
    private ReadyForQuery readyForQuery;

    public ResultSet() {
    }

    public void setRowDescription(RowDescription rowDescription) {
        this.rowDescription = rowDescription;
    }

    public void addDataRow(DataRow dataRow) {
        this.dataRows.add(dataRow);
    }

    public void setReadyForQuery(ReadyForQuery readyForQuery) {
        this.readyForQuery = readyForQuery;
    }

    public List<byte[]> toPackets() {
        validate();
        List<byte[]> packets = new LinkedList<>();
        packets.add(rowDescription.toPacket());
        dataRows.forEach(dataRow -> packets.add(dataRow.toPacket()));
        CommandComplete commandComplete =
            new CommandComplete(String.format("SELECT %d", dataRows.size()));
        packets.add(commandComplete.toPacket());
        packets.add(readyForQuery.toPacket());
        return packets;
    }

    /**
     * 校验已有信息能否构成完整的结果集数据包
     * 如果有数据包缺失,则抛出异常.
     *
     * @throws IllegalStateException
     */
    private void validate() {
        if (Objects.isNull(rowDescription)) {
            throw new IllegalStateException("no RowDescription");
        }
        if (Objects.isNull(readyForQuery)) {
            throw new IllegalStateException("no ReadyForQuery");
        }
    }
}
