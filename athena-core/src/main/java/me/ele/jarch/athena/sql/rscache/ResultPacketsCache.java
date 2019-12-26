package me.ele.jarch.athena.sql.rscache;

import me.ele.jarch.athena.scheduler.Scheduler;
import me.ele.jarch.athena.sql.CmdQuery;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by shaoyang.qi on 2017/9/13.
 * 此类用于暂存每一条需要缓存结果的sql的结果，待queryResultState结束时将结果缓存到scheduler中
 */
public class ResultPacketsCache {
    private static final int MAX_PACKET_SIZE = 100;
    private final CmdQuery curCmdQuery;
    private final List<byte[]> packets = new ArrayList<>();
    private boolean exceedMaxPktSize = false;

    public ResultPacketsCache(CmdQuery curCmdQuery) {
        this.curCmdQuery = curCmdQuery;
    }

    public void trySave(Scheduler scheduler) {
        if (exceedMaxPktSize) {
            return;
        }
        scheduler.queryResultCache.put(curCmdQuery.queryWithoutComment, this.packets);
    }

    public void tryAppendPacket(byte[] packet) {
        if (exceedMaxPktSize) {
            return;
        }

        if (this.packets.size() >= MAX_PACKET_SIZE) {
            exceedMaxPktSize = true;
            this.packets.clear();
            return;
        }
        this.packets.add(packet);
    }

}
