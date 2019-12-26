package me.ele.jarch.athena.detector;

import me.ele.jarch.athena.util.rmq.SlowSqlInfo;


/**
 * Created by rui.wang07 on 2017/10/16.
 */
public interface SlowSqlProducer {
    SlowSqlInfo pollSlowSql();
}
