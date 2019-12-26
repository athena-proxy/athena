package me.ele.jarch.athena.util;

import me.ele.jarch.athena.constant.Constants;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TransStatistics {
    public static final TransStatistics EMPTY = new EmptyTransStatistics();
    private static final String NO_RID = "no_rid";
    private List<QueryStatistics> querysStatistics = new ArrayList<>();
    private QueryStatistics lastQuerysStatistics = QueryStatistics.EMPTY;
    private long tStartTime;
    private long tEndTime;
    private final String transId;
    private final String rid;

    public TransStatistics(final String transId, final String rid) {
        this.transId = transId;
        this.rid = StringUtils.isEmpty(rid) ? NO_RID : rid;
    }

    public long getTransationDur() {
        long dur = 0;
        if (tStartTime == 0) {
            dur = 0;
        } else if (tEndTime == 0) {
            dur = System.currentTimeMillis() - tStartTime;
        } else {
            dur = tEndTime - tStartTime;
        }
        return dur;
    }

    public long gettStartTime() {
        return tStartTime;
    }

    public void settStartTime(long tStartTime) {
        this.tStartTime = tStartTime;
    }

    public long gettEndTime() {
        return tEndTime;
    }

    public void settEndTime(long tEndTime) {
        this.tEndTime = tEndTime;
    }

    public String getTransId() {
        return transId;
    }

    public String getRid() {
        return rid;
    }

    public void appendQueryStatistics(QueryStatistics queryStatistics) {
        this.lastQuerysStatistics = queryStatistics;
        this.querysStatistics.add(queryStatistics);
    }

    public QueryStatistics getlastQueryStatistics() {
        return lastQuerysStatistics;
    }

    public List<QueryStatistics> getQuerysStatistics() {
        return this.querysStatistics;
    }

    public String getTransTime() {
        StringBuilder sb = new StringBuilder(64);
        TimeFormatUtil.appendTime("tStart", sb, this.gettStartTime());
        TimeFormatUtil.appendTime("tEnd", sb, this.gettEndTime());
        return sb.toString();
    }

    static class EmptyTransStatistics extends TransStatistics {
        public EmptyTransStatistics() {
            super(Constants.DEFAULT_TRANS_ID, NO_RID);
        }

        public long gettStartTime() {
            return 0;
        }

        public void settStartTime(long tStartTime) {
        }

        public long gettEndTime() {
            return 0;
        }

        public void settEndTime(long tEndTime) {
        }

        public String getTransId() {
            return Constants.DEFAULT_TRANS_ID;
        }

        @Override public String getRid() {
            return NO_RID;
        }

        public void appendQueryStatistics(QueryStatistics query) {
        }

        public QueryStatistics getlastQueryStatistics() {
            return QueryStatistics.EMPTY;
        }

        public List<QueryStatistics> getQuerysStatistics() {
            return Collections.emptyList();
        }
    }
}
