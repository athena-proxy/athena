package me.ele.jarch.athena.server.async;

import me.ele.jarch.athena.allinone.DBConnectionInfo;
import me.ele.jarch.athena.allinone.HeartBeatResult;

/**
 * Created by zhengchao on 16/7/7.
 */
public interface HeartBeat {
    public void start();

    public void destroy();

    public DBConnectionInfo getDbConnInfo();

    public HeartBeatResult getResult();
}
