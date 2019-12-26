package com.github.mpjct.jmpjct.mysql.proto;

import me.ele.jarch.athena.util.proto.ResponseClassifier;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class MysqlResponseClassifier extends ResponseClassifier {

    public long sequenceId = 1;
    // public static long characterSet = 0;

    @Override public Optional<byte[]> getCommandComplete() {
        throw new RuntimeException("unsupport call `getCommandComplete` during MySQL protocol");
    }

    @Override public void setCommandComplete(byte[] commandComplete) {
        throw new RuntimeException("unsupport call `setCommandComplete` during MySQL protocol");
    }

    @Override public Optional<byte[]> getReadyForQuery() {
        throw new RuntimeException("unsupport call `getReadyForQuery` during MySQL protocol");

    }

    @Override public void setReadyForQuery(byte[] readyForQuery) {
        throw new RuntimeException("unsupport call `setReadyForQuery` during MySQL protocol");
    }

    @Override public List<byte[]> toPackets() {
        ArrayList<byte[]> packets = new ArrayList<byte[]>();

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

        EOF eof = new EOF();
        eof.sequenceId = this.sequenceId;
        this.sequenceId++;
        packets.add(eof.toPacket());

        for (byte[] row : this.rows) {
            System.arraycopy(Proto.build_fixed_int(1, this.sequenceId), 0, row, 3, 1);
            this.sequenceId++;
            packets.add(row);
        }

        eof = new EOF();
        eof.sequenceId = this.sequenceId;
        this.sequenceId++;
        packets.add(eof.toPacket());

        return packets;
    }
}
