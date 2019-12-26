package me.ele.jarch.athena.util;

import java.util.ArrayList;
import java.util.List;

public class QueryStatistics {
    static public final QueryStatistics EMPTY = new EmptyQueryStatistics();
    private long cRecvTime;
    private long sSendTime;
    private long sRecvTime;
    private long cSendTime;
    private boolean isShardedMode = false;
    private List<QueryStatistics> shardingQuerysStatistics = new ArrayList<>();
    private QueryStatistics currentQueryStatistics = this;

    public QueryStatistics() {
        super();
    }

    public long getProxyDur() {
        return cSendTime - cRecvTime;
    }

    /**
     * @return The duration for receiving all package of response
     */
    public long getServerDur() {
        long dur = 0;
        if (this.isShardedMode) {
            for (QueryStatistics qs : this.shardingQuerysStatistics) {
                dur += qs.getServerDur();
            }
        } else {
            dur = cSendTime - sSendTime;
        }
        return dur;
    }

    public long getcRecvTime() {
        return cRecvTime;
    }

    public void setcRecvTime(long cRecvTime) {
        this.cRecvTime = cRecvTime;
    }

    public long getsSendTime() {
        return sSendTime;
    }

    public void setsSendTime(long sSendTime) {
        this.sSendTime = sSendTime;
    }

    public long getsRecvTime() {
        return sRecvTime;
    }

    public void setsRecvTime(long sRecvTime) {
        this.sRecvTime = sRecvTime;
    }

    public long getcSendTime() {
        return cSendTime;
    }

    public void setcSendTime(long cSendTime) {
        this.cSendTime = cSendTime;
    }

    public String getProxyTime() {
        StringBuilder sb = new StringBuilder(64);
        TimeFormatUtil.appendTime("cRecv", sb, this.getcRecvTime());
        TimeFormatUtil.appendTime("cSend", sb, this.getcSendTime());
        return sb.toString();
    }

    public String toString() {
        StringBuilder sb = new StringBuilder(128);
        TimeFormatUtil.appendTime("cRecv", sb, this.getcRecvTime());
        TimeFormatUtil.appendTime("sSend", sb, this.getsSendTime());
        TimeFormatUtil.appendTime("sRecv", sb, this.getsRecvTime());
        TimeFormatUtil.appendTime("cSend", sb, this.getcSendTime());
        return sb.toString();
    }

    public boolean isShardedMode() {
        return isShardedMode;
    }

    public void setShardedMode(boolean isShardedMode) {
        this.isShardedMode = isShardedMode;
    }

    public void appendShardingQueryStatistics(QueryStatistics shardingQueryStatistics) {
        shardingQuerysStatistics.add(shardingQueryStatistics);
        this.currentQueryStatistics = shardingQueryStatistics;
    }

    public QueryStatistics getCurrentQueryStatistics() {
        return currentQueryStatistics;
    }

    static class EmptyQueryStatistics extends QueryStatistics {
        private EmptyQueryStatistics() {
            super();
        }

        @Override public long getcRecvTime() {
            return 0;
        }

        @Override public void setcRecvTime(long cRecvTime) {
        }

        @Override public long getsSendTime() {
            return 0;
        }

        @Override public void setsSendTime(long sSendTime) {
        }

        @Override public long getsRecvTime() {
            return 0;
        }

        @Override public void setsRecvTime(long sRecvTime) {
        }

        @Override public long getcSendTime() {
            return 0;
        }

        @Override public void setcSendTime(long cSendTime) {
        }

        @Override public boolean isShardedMode() {
            return false;
        }

        @Override public void setShardedMode(boolean isShardedMode) {
        }

        @Override public QueryStatistics getCurrentQueryStatistics() {
            return QueryStatistics.EMPTY;
        }
    }

}
