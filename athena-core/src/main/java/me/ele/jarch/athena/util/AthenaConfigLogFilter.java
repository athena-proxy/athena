package me.ele.jarch.athena.util;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by zhengchao on 2017/3/17.
 */
public class AthenaConfigLogFilter extends Filter<ILoggingEvent> {
    private static Set<String> ACCEPTED_LOGGER_NAMES = new HashSet<>();
    private static String[] LOGGER_NAMES_LIST =
        {"me.ele.jarch.athena.netty.AthenaServer", "me.ele.jarch.athena.netty.PreLoad",
            "me.ele.jarch.athena.netty.AthenaFrontServer",
            "me.ele.jarch.athena.netty.upgrade.UpgradeCenter",
            "me.ele.jarch.athena.netty.upgrade.UdsServer",
            "me.ele.jarch.athena.netty.upgrade.Downgrader",
            "me.ele.jarch.athena.netty.debug.DebugHttpServer",
            "me.ele.jarch.athena.netty.debug.FixedHttpServerInboundHandler",
            "me.ele.jarch.athena.netty.debug.HttpServerInboundHandler",
            "me.ele.jarch.athena.util.FilePingPong", "me.ele.jarch.athena.util.ZKCache",
            "me.ele.jarch.athena.util.GlobalZKCache", "me.ele.jarch.athena.util.CredentialsCfg",
            "me.ele.jarch.athena.util.DBChannelCfgMonitor",
            "me.ele.jarch.athena.util.GreySwitchCfg", "me.ele.jarch.athena.util.ZookeeperUtil",
            "me.ele.jarch.athena.util.AthenaConfig",
            "me.ele.jarch.athena.util.config.GlobalIdConfig",
            "me.ele.jarch.athena.util.deploy.OrgConfig", "me.ele.jarch.athena.util.gzs.GZSUtil",
            "me.ele.jarch.athena.util.shardkeystore.SqlPatternStore",
            "me.ele.jarch.athena.allinone.DBGroup", "me.ele.jarch.athena.allinone.AllInOne",
            "me.ele.jarch.athena.allinone.AllInOneMonitor",
            "me.ele.jarch.athena.allinone.AllInOneCodeHandler",
            "me.ele.jarch.athena.allinone.AllInOne", "me.ele.jarch.athena.sharding.ShardingRouter",
            "me.ele.jarch.athena.server.async.HeartBeatGroup",
            "me.ele.jarch.athena.util.curator.ZKCacheListener",
            "me.ele.jarch.athena.util.deploy.dalgroupcfg.DalGroupCfgFileLoader",
            "me.ele.jarch.athena.util.deploy.dalgroupcfg.DalGroupConfig",
            "me.ele.jarch.athena.util.deploy.dalgroupcfg.DalGroupConfigFetcher",
            "me.ele.jarch.athena.scheduler.DBChannelDispatcher",};

    static {
        Collections.addAll(ACCEPTED_LOGGER_NAMES, LOGGER_NAMES_LIST);
    }

    @Override public FilterReply decide(ILoggingEvent event) {
        String loggerName = event.getLoggerName();
        if (ACCEPTED_LOGGER_NAMES.contains(loggerName)) {
            return FilterReply.ACCEPT;
        }

        return FilterReply.DENY;
    }
}
