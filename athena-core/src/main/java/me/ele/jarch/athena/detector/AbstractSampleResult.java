package me.ele.jarch.athena.detector;

/**
 * Created by Dal-Dev-Team on 17/3/3.
 */
public abstract class AbstractSampleResult {
    protected double qps = 0;

    //    protected long upper90Dur = 0;

    // 窗口平均响应时间
    protected double avgDurInCurWindow = 0;

    // 历史平均响应时间
    protected double avgDurInAllWindow = 0;

    public double getQps() {
        return qps;
    }

    public void setQps(double qps) {
        this.qps = qps;
    }

    //    public long getUpper90Dur() {
    //        return upper90Dur;
    //    }
    //
    //    public void setUpper90Dur(long upper90Dur) {
    //        this.upper90Dur = upper90Dur;
    //    }

    public double getAvgDurInCurWindow() {
        return avgDurInCurWindow;
    }

    public void setAvgDurInCurWindow(double avgDurInCurWindow) {
        this.avgDurInCurWindow = avgDurInCurWindow;
    }

    public double getAvgDurInAllWindow() {
        return avgDurInAllWindow;
    }

    public void setAvgDurInAllWindow(double avgDurInAllWindow) {
        this.avgDurInAllWindow = avgDurInAllWindow;
    }

}
