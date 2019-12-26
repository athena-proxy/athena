package me.ele.jarch.athena.detector;

import me.ele.jarch.athena.netty.SqlSessionContext;

/**
 * Created by Dal-Dev-Team on 17/3/3.
 */
public abstract class AbstractSample {
    protected abstract void calc();

    public String dalGroup = "";

    // 窗口响应时间
    protected long durSumInCurWindow = 0;
    // 窗口个数
    protected long count = 0;

    // 当前窗口时间长度
    protected long curWindowDur = 0;

    public abstract void addSample(long dur, SqlSessionContext ctx);

    // duration分布
    //    private static final long[] DISTRIBUTE = { 1, 2, 3, 4, 5, 6, 7, 8, 10, 13, 17, 21, 27, 34, 44, 55, 71, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181, 6765,
    //            10946, 30000, 300000, Integer.MAX_VALUE };
    //    private long distribute[] = new long[DISTRIBUTE.length];
    //
    //    public long[] getDistribute() {
    //        return distribute;
    //    }

    public long getDurSumInCurWindow() {
        return durSumInCurWindow;
    }

    //    public void putDur(long _dur) {
    //        int dur = (int) _dur;
    //        for (int i = 0; i < DISTRIBUTE.length; i++) {
    //            if (dur <= DISTRIBUTE[i]) {
    //                distribute[i]++;
    //                return;
    //            }
    //        }
    //    }

    //    protected long calcUpper90() {
    //        if (distribute.length == 1) {
    //            return distribute[0];
    //        }
    //        long upper90_length = (long) Math.ceil(Arrays.stream(distribute).sum() * 0.9);
    //        if (upper90_length <= 0) {
    //            return 0;
    //        }
    //        // 如果值过小, 那么使用第二大的值,防止取到max
    //        if (distribute.length == upper90_length) {
    //            return distribute[distribute.length - 1];
    //        }
    //        for (int i = 0; i < distribute.length; i++) {
    //            if (upper90_length <= distribute[i]) {
    //                return DISTRIBUTE[i];
    //            } else {
    //                upper90_length -= distribute[i];
    //            }
    //        }
    //        return 0;
    //    }

    public long getCount() {
        return count;
    }

    public void setCurWindowDur(long curWindowDur) {
        this.curWindowDur = curWindowDur;
    }
}
