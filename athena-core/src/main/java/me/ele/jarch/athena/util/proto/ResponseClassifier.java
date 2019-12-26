package me.ele.jarch.athena.util.proto;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Created by jinghao.wang on 17/7/18.
 */
public abstract class ResponseClassifier {
    public final List<byte[]> columns = new ArrayList<>();
    public final List<byte[]> rows = new ArrayList<>();
    protected Optional<byte[]> commandCompleteOp = Optional.empty();
    protected Optional<byte[]> readyForQueryOp = Optional.empty();

    public void addColumn(byte[] column) {
        columns.add(column);
    }

    public void addRow(byte[] row) {
        rows.add(row);
    }

    public Optional<byte[]> getCommandComplete() {
        return commandCompleteOp;
    }

    public void setCommandComplete(byte[] commandComplete) {
        commandCompleteOp = Optional.ofNullable(commandComplete);
    }

    public Optional<byte[]> getReadyForQuery() {
        return readyForQueryOp;
    }

    public void setReadyForQuery(byte[] readyForQuery) {
        readyForQueryOp = Optional.ofNullable(readyForQuery);
    }

    public abstract List<byte[]> toPackets();
}
