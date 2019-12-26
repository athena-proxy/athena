package me.ele.jarch.athena.pg.proto;

import me.ele.jarch.athena.util.proto.ResponseClassifier;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jinghao.wang on 17/7/18.
 */
public class PGResponseClassifier extends ResponseClassifier {

    @Override public List<byte[]> toPackets() {
        List<byte[]> packets = new ArrayList<>();
        if (columns.size() == 1) {
            packets.add(columns.get(0));
        }
        packets.addAll(rows);
        commandCompleteOp.ifPresent(packets::add);
        readyForQueryOp.ifPresent(packets::add);
        return packets;
    }
}
