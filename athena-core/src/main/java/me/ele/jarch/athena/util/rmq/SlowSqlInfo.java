package me.ele.jarch.athena.util.rmq;

import java.util.Collections;
import java.util.Map;

/**
 * Created by rui.wang07 on 2017/10/16.
 */
public class SlowSqlInfo {
    public String dalGroup = "";
    public String clientAppId = "";
    public String reason = "";
    public String sqlId = "";
    public String sqlPattern = "";
    public String score = "";
    public Map<String, String> tags = Collections.emptyMap();
}
