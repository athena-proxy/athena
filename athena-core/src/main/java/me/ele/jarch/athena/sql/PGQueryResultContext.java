package me.ele.jarch.athena.sql;

import me.ele.jarch.athena.allinone.DBVendor;

/**
 * Created by zhengchao on 2017/2/20.
 */
public class PGQueryResultContext extends QueryResultContext {

    protected AbstractIntColumnWatcher createIntColumnWatcher() {
        return AbstractIntColumnWatcher.intColumnWatcherFactory(DBVendor.PG);
    }
}
