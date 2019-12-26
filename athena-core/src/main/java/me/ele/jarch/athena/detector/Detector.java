package me.ele.jarch.athena.detector;

import me.ele.jarch.athena.netty.SqlSessionContext;

/**
 * Created by Dal-Dev-Team on 17/3/9.
 */
public interface Detector {
    public Object statistics();

    public void startDetect();

    public void addSample(long dur, SqlSessionContext ctx);
}
