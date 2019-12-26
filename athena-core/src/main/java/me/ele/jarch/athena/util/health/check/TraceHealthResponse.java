package me.ele.jarch.athena.util.health.check;

import java.util.Map;

/**
 * Created by Tsai on 17/10/26.
 */
public class TraceHealthResponse {
    public boolean status;
    public long checkTime;
    public String desc;
    public Map<String, DalGroupIndicatorInfo> indicators;

    public TraceHealthResponse(boolean status, long checkTime, String desc,
        Map<String, DalGroupIndicatorInfo> indicators) {
        this.status = status;
        this.checkTime = checkTime;
        this.desc = desc;
        this.indicators = indicators;
    }
}
