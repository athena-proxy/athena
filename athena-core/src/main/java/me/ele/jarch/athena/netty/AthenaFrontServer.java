package me.ele.jarch.athena.netty;

import io.etrace.agent.Trace;
import io.etrace.common.Constants;
import me.ele.jarch.athena.constant.TraceNames;
import me.ele.jarch.athena.util.AthenaConfig;
import me.ele.jarch.athena.util.etrace.MetricFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by jinghao.wang on 16/9/19.
 */
public class AthenaFrontServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(AthenaFrontServer.class);

    private final AthenaTcpServer tcpServer = new AthenaTcpServer(new AthenaChInitializer(),
        AthenaEventLoopGroupCenter.getClientWorkerGroup(), true);
    private final int[] frontPorts = AthenaConfig.getInstance().getServicePorts();
    private final int[] pgPort = AthenaConfig.getInstance().getPgPort();

    /**
     * 防止重复调用close或open service_ports, 表明当前FrontServer是否接受新连接
     */
    private final AtomicBoolean acceptIncomeConnection = new AtomicBoolean(false);

    private AthenaFrontServer() {
    }

    private static class LazyHolder {
        private static final AthenaFrontServer FRONT_SERVER = new AthenaFrontServer();
    }

    public static AthenaFrontServer getInstance() {
        return LazyHolder.FRONT_SERVER;
    }

    public void bindFrontPorts() throws Exception {
        synchronized (this) {
            if (!acceptIncomeConnection.compareAndSet(false, true)) {
                return;
            }
            if (hasListenedPorts()) {
                return;
            }
            for (int port : frontPorts) {
                bindPort(port);
            }
            for (int port : pgPort) {
                bindPort(port);
            }
            Trace.logEvent(TraceNames.SERVICE_PORTS, TraceNames.UP, Constants.SUCCESS,
                buildProtocolServicePorts());
        }
    }

    public void closeBossChannel(long delayInMills) {
        synchronized (this) {
            if (!acceptIncomeConnection.compareAndSet(true, false)) {
                return;
            }
            AthenaServer.commonJobScheduler
                .addOneTimeJob("smooth down close service ports", delayInMills, () -> {
                    if (!acceptIncomeConnection.get()) {
                        tcpServer.closeBossChannel();
                        Trace.logEvent(TraceNames.SERVICE_PORTS, TraceNames.DOWN, Constants.SUCCESS,
                            buildProtocolServicePorts());
                    }
                });
        }
    }

    /**
     * 当前是否有服务端口处于监听状态
     *
     * @return
     */
    public boolean hasListenedPorts() {
        return tcpServer.getListenedPortsCount() > 0;
    }

    /**
     * 当前服务端口是否接受新连接
     * 注意:
     * 是否接收新连接与当前服务端口的监听状态无直接关联。
     * 在平滑下线的场景中,由于服务端口是延迟关闭的,但是
     * dal自己的心跳是期待即时失败的,所以存在有服务端口
     * 在监听,但是不接受新连接的情况
     *
     * @return
     */
    public boolean isAcceptInComeConnection() {
        return acceptIncomeConnection.get();
    }

    private void bindPort(int port) throws Exception {
        if (port % 2 != 1) {
            MetricFactory.newCounter("config.error").once();
            LOGGER.error(String.format(AthenaUtils.pidStr()
                + " the read/write dispatching port , %d, is not an odd value.", port));
            throw new IllegalArgumentException(
                String.format("front service port:%d must be odd,", port));
        }

        try {
            LOGGER.info(String
                .format(AthenaUtils.pidStr() + " starting dal binding on port %d/%d", port,
                    port + 1));
            tcpServer.tryStart(port);
            tcpServer.tryStart(port + 1);
            LOGGER.info(String
                .format(AthenaUtils.pidStr() + " is started and listening on port %d/%d", port,
                    port + 1));
            MetricFactory.newCounter("dal.group.up").once();
        } catch (Exception e) {
            LOGGER.error("ops!", e);
            MetricFactory.newCounter("config.error").once();
            LOGGER.error(String
                .format(AthenaUtils.pidStr() + " Failed to start on port, %d/%d", port, port + 1));
            throw e;
        }
    }

    private Map<String, String> buildProtocolServicePorts() {
        Map<String, String> protocolPorts = new LinkedHashMap<>();
        protocolPorts.put("MySQL", Arrays.toString(frontPorts));
        protocolPorts.put("PostgreSQL", Arrays.toString(pgPort));
        return protocolPorts;
    }
}
