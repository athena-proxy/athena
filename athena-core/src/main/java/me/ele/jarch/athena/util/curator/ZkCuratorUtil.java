package me.ele.jarch.athena.util.curator;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.netty.AthenaServer;
import me.ele.jarch.athena.scheduler.DBChannelDispatcher;
import me.ele.jarch.athena.util.GreySwitch;
import me.ele.jarch.athena.util.NoThrow;
import me.ele.jarch.athena.util.deploy.dalgroupcfg.AegisRquest;
import me.ele.jarch.athena.util.deploy.dalgroupcfg.DalGroupConfigFetcher;
import me.ele.jarch.athena.util.deploy.dalgroupcfg.Deployments;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;

public class ZkCuratorUtil {
    private static final Logger logger = LoggerFactory.getLogger(ZkCuratorUtil.class);

    private static final ObjectMapper objectMapper =
        new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);


    public static void trySetDalgroupCache(String dalgroup, String attribute, String newValue) {
        DBChannelDispatcher dispatcher = DBChannelDispatcher.getHolders().get(dalgroup);
        if (Objects.nonNull(dispatcher)) {
            dispatcher.getZKCache().setZkCfg(attribute, newValue);
        }
    }

    public static void setGlobalZkCache(String attribute, String newValue) {
        AthenaServer.globalZKCache.setZkCfg(attribute, newValue);
    }

    /**
     * 通过监听 zkPath 和 相应dalgroup znode的节点获取节点是否被加载的信息
     *
     * @param dalgroup
     */
    public static void tryLoadDalGroupDeployment(String dalgroup, String deployStr) {
        if (!GreySwitch.getInstance().zkDalgroupWatcherEnabled()) {
            return;
        }
        try {
            Deployments deployments = objectMapper.readValue(deployStr.trim(), Deployments.class);
            if (shouldLoad(dalgroup, deployments)) {
                DalGroupConfigFetcher.fetchDalGroupCfg(new AegisRquest(dalgroup, deployments));
            }
        } catch (Exception e) {
            logger.error("Got error while try Load DalGroupConfig for dalgroup {}", dalgroup, e);
        }
    }

    public static void initZKCache(DBChannelDispatcher dispatcher) {
        NoThrow.call(() -> {
            Map<String, ChildData> childrenData = ZkCurator.getAllChildrenData(dispatcher.getCfg());
            if (Objects.isNull(childrenData)) {
                logger.error("zk cache for dalgroup {} doesn't exist", dispatcher.getCfg());
                return;
            }
            childrenData.forEach(
                (attr, data) -> dispatcher.getZKCache().setZkCfg(attr, new String(data.getData())));
        });
    }

    private static boolean shouldLoad(String dalGroup, Deployments deployments) {
        return deployments.getDeploy().stream().anyMatch(deploy -> {
            if (!Constants.APPID.equals(deploy.getAppid())) {
                return false;
            }
            if (deploy.getHostnames().contains(Constants.HOSTNAME)) {
                return !DBChannelDispatcher.getHolders().containsKey(dalGroup)
                    || DBChannelDispatcher.getHolders().entrySet().stream().noneMatch(
                    entry -> entry.getKey().equals(dalGroup) && entry.getValue().getDalGroupConfig()
                        .getId().equals(deployments.getId()));
            } else {
                if (Deployments.DeployMode.RELEASE.equals(deployments.getDeployMode())) {
                    //The dalGroup was undelployed on this host, set it to offiline
                    DBChannelDispatcher.getHolders().entrySet().stream()
                        .filter(entry -> entry.getKey().equals(dalGroup)).findFirst()
                        .ifPresent(entry -> entry.getValue().offline());
                }
                return false;
            }
        });
    }

}
