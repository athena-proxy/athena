package me.ele.jarch.athena.netty.debug;

import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.netty.AthenaEventLoopGroupCenter;
import me.ele.jarch.athena.netty.AthenaTcpServer;
import me.ele.jarch.athena.netty.AthenaUtils;
import me.ele.jarch.athena.util.AthenaConfig;
import org.apache.http.HttpResponse;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Objects;

/**
 * Created by donxuu on 8/19/15.
 */
public class DebugHttpServer {
    private static final Logger log = LoggerFactory.getLogger(DebugHttpServer.class);
    private AthenaTcpServer dynamicTcpServer = null;

    private boolean canBindToPort(int port) throws IOException {
        RequestConfig.Builder builder = RequestConfig.custom();
        builder.setConnectionRequestTimeout(10);
        builder.setSocketTimeout(10);
        builder.setConnectTimeout(10);

        HttpGet httpGet = new HttpGet("http://127.0.0.1:" + port + "/ping");
        httpGet.setConfig(builder.build());

        CloseableHttpClient client = HttpClientBuilder.create().build();
        HttpResponse response = null;
        String body = null;
        try {
            response = client.execute(httpGet);
        } catch (IOException e) {
            log.info(AthenaUtils.pidStr() + " no one is listening to port {} {}", port,
                e.getMessage());
            return true;
        }

        try {
            ResponseHandler<String> handler = new BasicResponseHandler();
            body = handler.handleResponse(response);
            if (Objects.nonNull(body) && body.contains(Constants.APPID)) {
                log.info(AthenaUtils.pidStr() + " Port " + port + " is shared");
                return true;
            }
        } catch (IOException e) {
            log.error("failed to get debug server response!", e);
        } finally {
            client.close();
        }

        /*
         * Conventionally, the debug port is configured in athena.properties, or indirectly configured through eless. If a valid debug port has been specified,
         * we still try to bind to this port. If the debug port is 0, (usually this is in testing environment), we has to iterate to find a valid debug port.
         */
        return AthenaConfig.getInstance().getDebugPort() == port;
    }

    public void startDynamicServer(String appId) {
        int port = AthenaConfig.getInstance().getDebugPort() == 0 ?
            Constants.MIN_DYNAMIC_DEBUG_PORT :
            AthenaConfig.getInstance().getDebugPort();
        for (; port <= Constants.MAX_DYNAMIC_DEBUG_PORT; ++port) {
            try {
                if (canBindToPort(port)) {
                    dynamicTcpServer = new AthenaTcpServer(new DebugChInitializer(),
                        AthenaEventLoopGroupCenter.getDebugServerGroup(), true);
                    dynamicTcpServer.tryStart(port);
                    String hostName = Constants.FULL_HOSTNAME;
                    int idx = appId.lastIndexOf('.');
                    if (idx != -1) {
                        appId = appId.substring(idx + 1);
                    }
                    /*
                     * This will create the `Constants.DAL_DEBUG_SERVER_FILE` file, which is used for server debug info. Usually this file will be deleted when
                     * the jvm exits normally. When ELE_URGENCY_RELEASE is ON in eless, `kill -9` will be used to stop this jvm. At this time, this file won't
                     * be deleted. In order to make it consistent, the script `app-start.sh` is responsible for deleting the file when ELE_URGENCY_RELEASE is
                     * ON.
                     */
                    String filename = String.format(Constants.DAL_DEBUG_SERVER_FILE, appId);
                    try (BufferedWriter br = new BufferedWriter(new FileWriter(filename))) {
                        String content = String
                            .format("{\"appid\":\"%s\",\"host\":\"%s\",\"port\":%d}", appId,
                                hostName, port);
                        br.write(content);
                        br.newLine();
                    }

                    log.info(AthenaUtils.pidStr() + " binding debug port to " + port);
                    return;
                }
            } catch (Exception e) {
                if (port == AthenaConfig.getInstance().getDebugPort()) {
                    log.error("debug port has been used! conflict port: {} {}", port,
                        e.getMessage());
                    break;
                }
                log.error(AthenaUtils.pidStr() + " failed to bind debug port to " + port);
            }
        }

        log.error("can not find a debug port");
    }

    public void tryStartFixedServer() {
        HttpGet request = new HttpGet("http://127.0.0.1:8844/ping");
        RequestConfig.Builder builder = RequestConfig.custom();
        builder.setConnectionRequestTimeout(10);
        builder.setSocketTimeout(10);
        builder.setConnectTimeout(10);
        request.setConfig(builder.build());

        try (CloseableHttpClient client = HttpClientBuilder.create().build()) {
            client.execute(request).getStatusLine().getStatusCode();
            return;
        } catch (IOException e) {
            log.warn("8844 port not up {}", e.getMessage());
            // ignore, this means the 8844 port not up, }
            // start the debug port
            try {
                // 8844 port not reuse port, otherwise it will cause resource leak
                AthenaTcpServer server = new AthenaTcpServer(new FixedDebugChInitializer(),
                    AthenaEventLoopGroupCenter.getDebugServerGroup(), false);
                server.tryStart(Constants.FIXED_DEBUG_PORT);
            } catch (Exception e2) {
                log.info(
                    "Failed to start debug server on port : {}. The debug server may started in another instance. {}",
                    Constants.FIXED_DEBUG_PORT, e2.getMessage());
            }
        }
    }
}
