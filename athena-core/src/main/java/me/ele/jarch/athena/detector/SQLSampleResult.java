package me.ele.jarch.athena.detector;

/**
 * Created by Dal-Dev-Team on 17/3/3.
 */
public class SQLSampleResult extends AbstractSampleResult {
    protected long avgReqBytesInCurWindow = 0;
    protected long avgRespBytesInCurWindow = 0;
    protected long avgAffectedRowsInCurWindow = 0;

    public long getAvgReqBytesInCurWindow() {
        return avgReqBytesInCurWindow;
    }

    public void setAvgReqBytesInCurWindow(long avgReqBytesInCurWindow) {
        this.avgReqBytesInCurWindow = avgReqBytesInCurWindow;
    }

    public long getAvgRespBytesInCurWindow() {
        return avgRespBytesInCurWindow;
    }

    public void setAvgRespBytesInCurWindow(long avgRespBytesInCurWindow) {
        this.avgRespBytesInCurWindow = avgRespBytesInCurWindow;
    }

    public long getAvgAffectedRowsInCurWindow() {
        return avgAffectedRowsInCurWindow;
    }

    public void setAvgAffectedRowsInCurWindow(long avgAffectedRowsInCurWindow) {
        this.avgAffectedRowsInCurWindow = avgAffectedRowsInCurWindow;
    }
}
