package me.ele.jarch.athena.util.curator;

import me.ele.jarch.athena.netty.Monitorable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CuratorMonitorJob implements Monitorable {
    private static final Logger logger = LoggerFactory.getLogger(CuratorMonitorJob.class);

    @Override public void monitor() {
        if (ZkCurator.isConnected()) {
            return;
        }
        ZkCurator.close();
        logger.error("start to re-init curator since curator connection is lost");
        ZkCurator.initCurator();
    }
}
