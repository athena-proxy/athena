package me.ele.jarch.athena.util.curator;

import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.constant.Metrics;
import me.ele.jarch.athena.constant.TraceNames;
import me.ele.jarch.athena.util.AthenaConfig;
import me.ele.jarch.athena.util.etrace.MetricFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

import static me.ele.jarch.athena.constant.Constants.APPID;
import static org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type.CONNECTION_RECONNECTED;
import static org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type.INITIALIZED;

public class ZKCacheListener implements TreeCacheListener {
    private static final Logger logger = LoggerFactory.getLogger(ZKCacheListener.class);

    public AtomicInteger suspendCount = new AtomicInteger(0);

    @Override public void childEvent(CuratorFramework client, TreeCacheEvent event)
        throws Exception {
        switch (event.getType()) {
            case NODE_ADDED:
            case NODE_UPDATED:
                updateCache(event.getData());
                break;

            case NODE_REMOVED:
                doNodeRemoved(event.getData());
                break;

            // 对ZK连接相关的event， 发送Metric打点用作告警
            case CONNECTION_LOST:
                ZkCurator.close();
                ZkCurator.setInitialized(false);
                break;
            case CONNECTION_SUSPENDED:
                MetricFactory.newCounter(Metrics.ZK_CONNECTION)
                    .addTag(TraceNames.STATUS, event.getType().toString()).once();
                logger.error("{} curator server", event.getType().toString());
                if (suspendCount.getAndIncrement() < 10) {
                    return;
                }
                ZkCurator.close();
                ZkCurator.setInitialized(false);
                break;

            case CONNECTION_RECONNECTED:
                MetricFactory.newCounter(Metrics.ZK_CONNECTION)
                    .addTag(TraceNames.STATUS, CONNECTION_RECONNECTED.toString()).once();
                logger.error("reconnected to curator server");
                break;
            case INITIALIZED:
                ZkCurator.setInitialized(true);
                MetricFactory.newCounter(Metrics.ZK_CONNECTION)
                    .addTag(TraceNames.STATUS, INITIALIZED.toString()).once();
                logger.error("success!connected to curator.");
                break;

            default:
                break;
        }
    }

    private void doNodeRemoved(ChildData data) {
        String path = data.getPath();
        if (isAttributeChanged(path)) {
            String attr = ZKPaths.getNodeFromPath(path);
            if (attr.equals(Constants.REJECT_SQL_BY_PATTERN) || attr
                .equals(Constants.REJECT_SQL_BY_REGULAR_EXP)) {
                String dalgroup = getParentNode(path);
                ZkCuratorUtil.trySetDalgroupCache(dalgroup, attr, "");
            }
        } else {
            logger
                .info("path {} is removed, data is {}", data.getPath(), new String(data.getData()));
        }
    }

    private void updateCache(ChildData data) {
        String path = data.getPath();
        //cache只关心dalgroup属性变更, 属性的上层节点的事件都忽略
        if (isAttributeChanged(path)) {
            String parent = getParentNode(path);
            String attribute = ZKPaths.getNodeFromPath(path);
            if (APPID.equals(parent)) {
                ZkCuratorUtil.setGlobalZkCache(attribute, new String(data.getData()));
                // zk heartbeat打点，报警统计为count(zk.heartbeat)
                if (Constants.HEARTBEAT.equals(attribute)) {
                    MetricFactory.newCounter(Metrics.ZK_HEARTBEAT).once();
                }
                return;
            }
            if (attribute.equals(Constants.CONFIG_VERSION)) {
                ZkCuratorUtil.tryLoadDalGroupDeployment(parent, new String(data.getData()));
                return;
            }
            ZkCuratorUtil.trySetDalgroupCache(parent, attribute, new String(data.getData()));
        } else {
            logger
                .info("path {} is changed, data is {}", data.getPath(), new String(data.getData()));
        }
    }

    private String getParentNode(String path) {
        String parentPath = ZKPaths.getPathAndNode(path).getPath();
        return ZKPaths.getNodeFromPath(parentPath);
    }

    /**
     * 由于dal监听的path格式为 {zookeeperPath}/dalgroup/attribute, cache只关心attribute的变化，所以只响应同样深度的path变更
     *
     * @param path
     * @return
     */
    private boolean isAttributeChanged(String path) {
        int parentDepth = AthenaConfig.getInstance().getZookeeperPath().split("/").length;
        return ZKPaths.split(path).size() == parentDepth + 1;
    }
}
