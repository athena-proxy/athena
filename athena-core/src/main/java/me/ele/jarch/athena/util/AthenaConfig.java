package me.ele.jarch.athena.util;

import io.etrace.agent.config.AgentConfiguration;
import io.etrace.agent.config.ConfigurationLoader;
import me.ele.jarch.athena.constant.Constants;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.*;

public class AthenaConfig {

    private static final Logger logger = LoggerFactory.getLogger(AthenaConfig.class);
    private String databaseConnectString = "";
    private int[] servicePorts;
    private int debugPort;
    private int[] pgPort;
    private int workingThreadsCount = 4;

    private int nettyThreadCount = 4;
    private int asyncThreadCount = 2;

    private String connectPath;
    private String zookeeperPath;

    private String dalSeqsTable;
    private int dalSeqsCachePool;

    private GrayType grayType = GrayType.SHARDING;
    private String is_send_abort = "1";

    public Properties getConfig() {
        return config;
    }

    private final Properties config = new Properties();

    private volatile Set<String> dbCfgs = new HashSet<>();
    private String globalConfigPath = "/";
    private String globalLogPath = "/";

    private int mulschedulerCapacity = 1024;
    private int mulschedulerWorkerCount = 1;

    private int thresholdClientCount = 20000;

    private String smoothDownPwd = "";

    private String shardKeyDbConnStr = "";

    private String loopbackIP;
    private String dalAdminReadonlyPwd;

    private String httpAsyncClientMaxThread = "";

    private String aegisHttpUrl = "";
    private String amqpUri = "";

    public String getAllInOneFile() {
        return Paths.get(getInstance().getGlobalConfigPath(), "db-connections.cfg").toString();
    }

    public String getAdditionalPortFile() {
        return Paths.get(getGlobalConfigPath(), "dal-proxy-ports.cfg").toString();
    }

    public String getCredentialFile() {
        return Paths.get(getGlobalConfigPath(), "dal-credentials.cfg").toString();
    }

    public String getShardingConfigPath() {
        return Paths.get(getGlobalConfigPath(), "sharding.yml").toString();
    }

    public String getGreySwitchConfigPath() {
        return Paths.get(getGlobalConfigPath(), "dal-grey-switch.cfg").toString();
    }

    public String getGlobalIdsConfigPath() {
        return Paths.get(getGlobalConfigPath(), "dal-globalids.yml").toString();
    }

    static class INNER {
        static AthenaConfig athenaConfig = new AthenaConfig();
    }

    public static AthenaConfig getInstance() {
        return INNER.athenaConfig;
    }

    private void loadCfgNormal() {
        final String propFile = "conf/athena.properties";
        try (InputStream athenaConfig = getClass().getClassLoader().getResourceAsStream(propFile)) {
            config.load(athenaConfig);
        } catch (IOException e) {
            logger.error("failed to load configuration file: " + propFile, e);
        }
    }

    public void load() throws Exception {
        // load normal config to properties
        loadCfgNormal();
        // set variable to memeory variable
        setAllProperty();
        loadCredentialsCfg();
    }


    private void loadCredentialsCfg() throws FileNotFoundException {
        CredentialsCfg.loadConfig(
            Paths.get(AthenaConfig.getInstance().getGlobalConfigPath(), "dal-credentials.cfg")
                .toString());
    }

    // set properties to memory variable
    private void setAllProperty() {
        dalSeqsTable = config.getProperty(Constants.DAL_SEQUENCE_TABLE, "dal_sequences").trim();
        dalSeqsCachePool =
            Integer.valueOf(config.getProperty(Constants.DAL_SEQUENCE_CACHE_POLL, "1000").trim());

        connectPath = config.getProperty("connect_path").trim() + "/" + EnvConf.get().idc();
        zookeeperPath = config.getProperty("zookeeper_path").trim();

        workingThreadsCount = Integer.valueOf(config.getProperty("working_threads_count").trim());

        nettyThreadCount = Integer.valueOf(config.getProperty("netty_thread_count").trim());
        asyncThreadCount = Integer.valueOf(config.getProperty("async_thread_count").trim());
        servicePorts = decodeServicePorts(config.getProperty("service_port"));
        debugPort = Integer.valueOf(config.getProperty("debug_port"));
        pgPort = decodeServicePorts(config.getProperty("pg_port"));
        globalConfigPath = config.getProperty("global_config_path");
        globalLogPath = globalConfigPath.replace("conf", "log");

        is_send_abort = config.getProperty("is_send_abort", "1").trim();

        mulschedulerCapacity = Integer.valueOf(config.getProperty("mulscheduler_capacity"));
        mulschedulerWorkerCount = Integer.valueOf(config.getProperty("mulscheduler_worker_count"));

        thresholdClientCount =
            Integer.valueOf(config.getProperty("threshold_client_count", "20000").trim());
        smoothDownPwd = config.getProperty("smooth_down_password").trim();
        shardKeyDbConnStr = config.getProperty("shard_key_db_conn_str", "");

        loopbackIP = config.getProperty("loopback_ip", "127.0.0.1");
        dalAdminReadonlyPwd =
            new String((Base64.decodeBase64(config.getProperty("athena_admin_readonly_pwd", ""))),
                StandardCharsets.UTF_8);

        httpAsyncClientMaxThread = config.getProperty("http_async_client_max_thread", "").trim();
        aegisHttpUrl = config.getProperty("aegis_http_url", "").trim();
        amqpUri = config.getProperty("amqp_uri", "").trim();

        AgentConfiguration.initByConfigurationLoader(new ConfigurationLoader() {
            @Override public String getAppId() {
                return Constants.APPID;
            }

            @Override public String getCollectorDomainAndPort() {
                return config.getProperty("etrace_collector_ip");
            }

            @Override public String getTenant() {
                return "default";
            }

            @Override public String getInstance() {
                return "default-instance";
            }

            @Override public Map<String, String> getGlobalTags() {
                return Collections.emptyMap();
            }

            @Override public Set<String> getBeanObjectNames() {
                return Collections.emptySet();
            }

            @Override public Map<String, String> getExtraProperties() {
                return Collections.emptyMap();
            }
        });
    }

    private int[] decodeServicePorts(String service_port) {
        String[] ports = service_port.split(",");
        return Arrays.asList(ports).stream().filter(i -> i != null && !i.trim().isEmpty())
            .mapToInt(i -> Integer.parseInt(i.trim())).toArray();

    }

    public Set<String> getDbCfgs() {
        return dbCfgs;
    }

    public void setDbCfgs(Set<String> dbCfgs) {
        this.dbCfgs = dbCfgs;
    }

    public String getDatabaseConnectString() {
        return databaseConnectString;
    }

    public int getWorkingThreadsCount() {
        return workingThreadsCount;
    }

    public int getNettyThreadCount() {
        return nettyThreadCount;
    }

    public int getAsyncThreadCount() {
        return asyncThreadCount;
    }

    public int[] getServicePorts() {
        if (servicePorts.length == 0) {
            logger.error("No Valid mysql listening port defined.");
        }
        return servicePorts;
    }

    public int getDebugPort() {
        return debugPort;
    }

    public int[] getPgPort() {
        if (pgPort.length == 0) {
            logger.warn("No Valid PG listening port defined");
        }
        return pgPort;
    }

    public String getConnectPath() {
        return connectPath;
    }

    public String getZookeeperPath() {
        return zookeeperPath;
    }

    public int getDalSeqsCachePool() {
        return dalSeqsCachePool;
    }

    public String getGlobalConfigPath() {
        return globalConfigPath;
    }

    public GrayType getGrayType() {
        return grayType;
    }

    public void setGrayType(GrayType grayType) {
        this.grayType = grayType;
    }

    public boolean isSendAbort() {
        if (is_send_abort.equals("0")) {
            return false;
        }
        return true;
    }

    public void setDatabaseConnectString(String databaseConnectString) {
        if (databaseConnectString != null) {
            this.databaseConnectString = databaseConnectString;
        }

    }

    public int getMulschedulerCapacity() {
        return mulschedulerCapacity;
    }

    public void setMulschedulerCapacity(int mulschedulerCapacity) {
        this.mulschedulerCapacity = mulschedulerCapacity;
    }

    public int getMulschedulerWorkerCount() {
        return mulschedulerWorkerCount;
    }

    public void setMulschedulerWorkerCount(int mulschedulerWorkerCount) {
        this.mulschedulerWorkerCount = mulschedulerWorkerCount;
    }

    public String getDalSeqsTable() {
        return dalSeqsTable;
    }

    public String getOrgFile() {
        return Paths.get(getGlobalConfigPath(), "goproxy-front-port.cfg").toString();
    }

    public String getGlobalLogPath() {
        return globalLogPath;
    }

    public int getThresholdClientCount() {
        return thresholdClientCount;
    }

    public String getSmoothDownPwd() {
        return smoothDownPwd;
    }

    public String getShardKeyDbConnStr() {
        return shardKeyDbConnStr;
    }

    public void setShardKeyDbConnStr(String shardKeyDbConnStr) {
        this.shardKeyDbConnStr = shardKeyDbConnStr;
    }

    public String getLoopbackIP() {
        return loopbackIP;
    }

    public String getDalAdminReadonlyPwd() {
        return dalAdminReadonlyPwd;
    }

    public String getHttpAsyncClientMaxThread() {
        return httpAsyncClientMaxThread;
    }

    public String getAegisHttpUrl() {
        return aegisHttpUrl;
    }

    public String getAmqpUri() {
        return amqpUri;
    }
}
