package me.ele.jarch.athena.netty.debug;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.HttpHeaders.Values;
import io.netty.util.ReferenceCountUtil;
import me.ele.jarch.athena.allinone.*;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.detector.DetectorDelegate;
import me.ele.jarch.athena.netty.AthenaFrontServer;
import me.ele.jarch.athena.netty.AthenaUtils;
import me.ele.jarch.athena.netty.SmoothDownDoorman;
import me.ele.jarch.athena.scheduler.DBChannelDispatcher;
import me.ele.jarch.athena.sql.seqs.GlobalId;
import me.ele.jarch.athena.util.*;
import me.ele.jarch.athena.util.etrace.EtracePatternUtil;
import me.ele.jarch.athena.util.rmq.AuditSqlToRmq;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static io.netty.handler.codec.http.HttpHeaders.Names.*;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;


public class HttpServerInboundHandler extends ChannelInboundHandlerAdapter {
    private static final Logger log = LoggerFactory.getLogger(HttpServerInboundHandler.class);
    private HttpRequest request;

    private FullHttpResponse buildResponse(String res) throws UnsupportedEncodingException {
        return buildResponse(res, OK);
    }

    private FullHttpResponse buildResponse(String res, HttpResponseStatus status)
        throws UnsupportedEncodingException {
        FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, status,
            Unpooled.wrappedBuffer(res.getBytes("UTF-8")));
        response.headers().set(CONTENT_TYPE, "text/plain");
        response.headers().set(CONTENT_LENGTH, response.content().readableBytes());
        if (HttpHeaders.isKeepAlive(request)) {
            response.headers().set(CONNECTION, Values.KEEP_ALIVE);
        }
        return response;
    }

    @Override public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        try {
            if (msg instanceof HttpRequest) {
                request = (HttpRequest) msg;
                String uri = request.getUri();
                if ("/ping".equalsIgnoreCase(uri)) {
                    HttpResponseStatus status = OK;
                    String content = "1\n\n" + Constants.APPID + "\n";
                    HealthCheck healthCheck = new HealthCheck();
                    if (!healthCheck.check()) {
                        status = new HttpResponseStatus(502, " Proxy Error");
                        content = "0\n\n" + Constants.APPID + "\n" + healthCheck.getCheckMessage();
                    }
                    FullHttpResponse response = buildResponse(content, status);
                    ctx.writeAndFlush(response);
                    return;
                }

                if (uri.startsWith("/service_ports/close?")) {
                    Map<String, String> kvPair =
                        KVChainParser.parse(uri.substring(21, uri.length()));
                    String token = kvPair.getOrDefault("token", "emptytoken");
                    FullHttpResponse response = handleCloseServicePorts(token);
                    ctx.writeAndFlush(response);
                    return;
                }

                if (uri.startsWith("/service_ports/open?")) {
                    Map<String, String> kvPair =
                        KVChainParser.parse(uri.substring(20, uri.length()));
                    String token = kvPair.getOrDefault("token", "emptytoken");
                    FullHttpResponse response = handleOpenServicePorts(token);
                    ctx.writeAndFlush(response);
                    return;
                }
                if (uri.startsWith("/rmq_channel/reset")) {
                    handleRmqChannelReset(ctx);
                    return;
                }
                if ("/sqlpattern".equalsIgnoreCase(uri)) {
                    HttpResponseStatus status = OK;
                    String content = EtracePatternUtil.sqlPatternInfo();
                    FullHttpResponse response = buildResponse(content, status);
                    ctx.writeAndFlush(response);
                    return;
                }
                if (uri.startsWith("/heartbeat?")) {
                    HeartBeatCenter.config(uri);
                    ctx.writeAndFlush(buildResponse("try config heartbeat"));
                    return;
                }
                if (uri.startsWith("/dbstatus")) {
                    FullHttpResponse response = handleDBStatus(uri);
                    ctx.writeAndFlush(response);
                    return;
                }
                if (uri.equalsIgnoreCase("/globalids")) {
                    FullHttpResponse response = buildGlobalIdsResponse();
                    ctx.writeAndFlush(response);
                    return;
                }

                if (uri.startsWith("/samples")) {
                    FullHttpResponse response = handleSamples();
                    ctx.writeAndFlush(response);
                    return;
                }

                // 通过浏览器访问debug端口时，会自动发送favicon.ico请求，拦截
                if (uri.startsWith("/favicon.ico")) {
                    return;
                }

                if ("/health".equalsIgnoreCase(uri)) {
                    FullHttpResponse response = buildDalGroupHealthCheckResponse();
                    ctx.writeAndFlush(response);
                    return;
                }
                //查询dalgroup版本信息，格式为:/dalgroup/version?dalgroupName=test, 若不加dalgroupName参数则返回所有dalgroup版本信息
                if (uri.startsWith("/dalgroup/version")) {
                    FullHttpResponse response = queryDalgroupVersion(uri);
                    ctx.writeAndFlush(response);
                    return;
                }

                //根据dalgroup snapshotid查询此版本dalgroup是否加载成功，且DB心跳正常，格式为:/dal_group？dalgroupName=test&snapshotId=123
                if (uri.startsWith("/dal_group")) {
                    FullHttpResponse response = handleDalGroupStatus(uri);
                    ctx.writeAndFlush(response);
                    return;
                }

                /**
                 * handle default url "/" or unknown url
                 */
                ctx.writeAndFlush(handleDefaultUrl());
            }
        } finally {
            ReferenceCountUtil.release(msg);
        }
    }

    private void handleRmqChannelReset(ChannelHandlerContext ctx)
        throws UnsupportedEncodingException {
        AuditSqlToRmq.getInstance().resetChannelOfRmqSender();
        HttpResponseStatus status = OK;
        String content = "reset channel of RmqSender success!";
        FullHttpResponse response = buildResponse(content, status);
        ctx.writeAndFlush(response);
    }

    @Override public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

    @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error(cause.getMessage(), cause);
        ctx.close();
    }

    private FullHttpResponse handleCloseServicePorts(String token) throws Exception {
        if (!SmoothDownDoorman.getInstance().allowSmoothDown(token)) {
            log.error("unauthorized access to close service ports with token {}", token);
            return buildResponse("You don't have permission to access /service_ports/close\n",
                FORBIDDEN);
        }
        log.info("smooth down, {} closing all service ports ... ", AthenaUtils.pidStr());
        try {
            AthenaFrontServer.getInstance().closeBossChannel(Constants.SMOOTH_DOWN_DELAY_IN_MILLS);
            return buildResponse(String
                .format("appid: %s, hostname: %s, pid: %s service ports closed\n", Constants.APPID,
                    Constants.HOSTNAME, Constants.PID), OK);
        } catch (Exception e) {
            log.error("Unexpected exception when close all service ports", e);
            return buildResponse("Close all service ports failure!", INTERNAL_SERVER_ERROR);
        }
    }

    private FullHttpResponse handleOpenServicePorts(String token)
        throws UnsupportedEncodingException {
        if (!SmoothDownDoorman.getInstance().allowSmoothDown(token)) {
            log.error("unauthorized access to open service ports with token {}", token);
            return buildResponse("You don't have permission to access /service_ports/open\n",
                FORBIDDEN);
        }
        log.info("{} Opening all service ports ... ", AthenaUtils.pidStr());
        try {
            AthenaFrontServer.getInstance().bindFrontPorts();
            return buildResponse(String
                .format("appid: %s, hostname: %s, pid: %s service ports opened\n", Constants.APPID,
                    Constants.HOSTNAME, Constants.PID), OK);
        } catch (Exception e) {
            log.error("Unexpected exception when open all service ports", e);
            return buildResponse("Open all service ports failure!", INTERNAL_SERVER_ERROR);
        }
    }

    private String printDBStatusHead() {
        StringBuilder builder = new StringBuilder();
        builder.append(String.format("%16s ", "DalGroup"));
        builder.append(String.format("%16s ", "DBGroup"));
        builder.append(String.format("%7s ", "DBRole"));
        builder.append(String.format("%6s ", "Status"));
        builder.append(String.format("%9s ", "ReadOnly"));
        builder.append(String.format("%15s ", "Host"));
        builder.append(String.format("%5s ", "Port"));
        builder.append(String.format("%11s ", "Database"));
        builder.append(String.format("%8s ", "ID"));
        builder.append(String.format("%11s", "Attributes"));
        builder.append("\n");
        IntStream.range(0, 113).forEach(i -> builder.append("="));
        builder.append("\n");
        return builder.toString();
    }

    private String printDalGroup(DBChannelDispatcher dispatcher, String dbGroupName) {
        StringBuilder builder = new StringBuilder();
        dispatcher.getDbGroups().values().stream().filter(db -> {
            return dbGroupName.isEmpty() || db.getGroupName().equals(dbGroupName);
        }).forEach(dbGroup -> {
            DbGroupUpdater updater = dbGroup.getDbGroupUpdater();
            updater.getAllInfos().values().stream()
                .sorted((a, b) -> a.getRole().compareTo(b.getRole())).forEach(info -> {
                builder.append(String.format("%16s ", dispatcher.getDalGroup().getName()));
                builder.append(String.format("%16s ", info.getGroup()));

                String role = info.getRole().toString();
                if (info.getRole() == DBRole.MASTER && info.isShadowDb())
                    role = "SHADOW";
                builder.append(String.format("%7s ", role));

                String status = "offline";
                HeartBeatResult heartBeatResult = HeartBeatCenter.getInstance().getDBStatus(info);
                if (updater.isOnline(info)) {
                    boolean isActive = false;
                    if (Objects.nonNull(heartBeatResult)) {
                        isActive = heartBeatResult.isAlive();
                    }
                    if (isActive) {
                        status = "alive";
                        if (info.isActive()) {
                            status = "active";
                        }
                        if (dispatcher.getSickMonitor().isSicked(info.getQualifiedDbId()))
                            status = "sick";
                    } else {
                        status = "dead";
                    }
                }

                builder.append(String.format("%6s ", status));
                boolean isReadOnly = true;
                if (Objects.nonNull(heartBeatResult))
                    isReadOnly = heartBeatResult.isReadOnly();
                builder.append(String.format("%9s ", isReadOnly));
                builder.append(String.format("%15s ", info.getHost()));
                builder.append(String.format("%5s ", info.getPort()));
                builder.append(String.format("%11s ", info.getDatabase()));
                builder.append(String.format("%8s ", info.getId()));
                builder.append(String.format("%11s", info.getAttributesString()));
                builder.append("\n");
            });
            IntStream.range(0, 113).forEach(i -> builder.append("-"));
            builder.append("\n");
        });
        return builder.toString();
    }

    /**
     * 返回DB的当前状态，支持restful形式的查询，格式 /dbstatus/dalGroupName/dbGroupName 1. 如果 uri 为 /dbstatus, 返回所有DB的状态 2. 如果 uri 为 /dbstatus/dalGroup，返回名为dalGroup所对应的所有DB状态 3.
     * 如果 uri 为 /dbstatus/dalGroup/dbGroup，返回名为dalGroup对应的名为dbGroup的所有DB状态
     *
     * @param uri
     * @return
     * @throws UnsupportedEncodingException
     */
    private FullHttpResponse handleDBStatus(String uri) throws UnsupportedEncodingException {
        StringBuilder builder = new StringBuilder();
        builder.append(printDBStatusHead());
        if (uri.equalsIgnoreCase("/dbstatus") || uri.equalsIgnoreCase("/dbstatus/")) {
            DBChannelDispatcher.getHolders().values().forEach(dispatcher -> {
                builder.append(printDalGroup(dispatcher, ""));
            });
        } else if (uri.startsWith("/dbstatus/") && uri.length() > "/dbstatus/".length()) {
            String dalname = uri.substring("/dbstatus/".length());
            String dbname = "";
            if (dalname.contains("/")) {
                String dalname_tmp = dalname.substring(0, dalname.indexOf('/'));
                dbname = dalname.substring(dalname.indexOf('/') + 1);
                dalname = dalname_tmp;
            }
            // final for lambda
            final String dalGroupName = dalname;
            final String dbGroupName = dbname;
            DBChannelDispatcher.getHolders().values().stream()
                .filter(dispatcher -> dispatcher.getDalGroup().getName().equals(dalGroupName))
                .forEach(dispatcher -> {
                    builder.append(printDalGroup(dispatcher, dbGroupName));
                });
        } else {
            return buildResponse(
                "Error when parse uri, use \"/dbstatus/<DalGroupName>/<DBGroupName>\" to query.");
        }
        try {
            return buildResponse(builder.toString());
        } catch (Exception e) {
            log.error("Exception in handleDBStatus()", e);
            return buildResponse(e.toString());
        }
    }

    private FullHttpResponse queryDalgroupVersion(String uri) throws UnsupportedEncodingException {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode rootNode = mapper.createObjectNode();
        ObjectNode data = mapper.createObjectNode();

        Map<String, List<String>> paramMap = new QueryStringDecoder(uri).parameters();
        if (paramMap.containsKey("dalGroupName") && !paramMap.get("dalGroupName").isEmpty()) {
            String dalgroup = paramMap.get("dalGroupName").get(0);
            DBChannelDispatcher dispatcher = DBChannelDispatcher.getHolders().get(dalgroup);
            if (Objects.isNull(dispatcher)) {
                data.put(dalgroup, "NOT_EXIST");
            } else {
                data.put(dalgroup, dispatcher.getDalGroupConfig().getId());
            }
        }
        if (paramMap.isEmpty()) {
            DBChannelDispatcher.getHolders().forEach((dalgroup, dispatcher) -> data
                .put(dalgroup, dispatcher.getDalGroupConfig().getId()));
        }
        rootNode.put("host", Constants.HOSTNAME);
        rootNode.put("appid", Constants.APPID);

        if (data.size() > 0) {
            rootNode.put("success", "true");
            rootNode.set("data", data);
            rootNode.put("msg", "Query succeed");
        } else {
            rootNode.put("success", "false");
            rootNode.put("msg", "Can't find related dalgroup on this host");
        }
        String response;
        try {
            response = mapper.writeValueAsString(rootNode);
        } catch (JsonProcessingException e) {
            log.error("Error generating json string", e);
            response = "\"success\": \"false\"," + "\"msg\": \"DAL internal error\"";
        }
        return buildResponse(response);
    }

    private FullHttpResponse handleDalGroupStatus(String uri) throws UnsupportedEncodingException {
        Map<String, List<String>> paramMap = new QueryStringDecoder(uri).parameters();
        if (!paramMap.containsKey("dalGroupName") || !paramMap.containsKey("snapshotId")) {
            return buildAegisHCResp("", false, "dalgroup or id is missing");
        }
        String dalgroup = paramMap.get("dalGroupName").get(0);
        String dalGroupId = paramMap.get("snapshotId").get(0);

        if (!DBChannelDispatcher.getHolders().containsKey(dalgroup)) {
            return buildAegisHCResp(dalGroupId, false,
                "Dalgroup " + dalgroup + " is not loaded yet");
        }

        if (!DBChannelDispatcher.getHolders().get(dalgroup).getDalGroupConfig().getId()
            .equals(dalGroupId)) {
            return buildAegisHCResp(dalGroupId, false,
                "DalgroupId " + dalGroupId + " is not loaded yet");
        }

        if (checkDbStatus(dalgroup)) {
            return buildAegisHCResp(dalGroupId, true,
                "DalgroupId " + dalGroupId + " loaded successfully");
        } else {
            return buildAegisHCResp(dalGroupId, false,
                "Db status is not OK for dalgroupId " + dalGroupId);
        }
    }

    private boolean checkDbStatus(String dalgroup) {
        AtomicBoolean isSuccess = new AtomicBoolean(false);
        NoThrow.call(() -> {
            boolean isDbActive =
                DBChannelDispatcher.getHolders().get(dalgroup).getDbGroups().values().stream()
                    .allMatch(dbGroup -> dbGroup.getDbGroupUpdater().getAllInfos().values().stream()
                        .filter(dbInfo -> dbGroup.getDbGroupUpdater().isOnline(dbInfo))
                        .allMatch(DBConnectionInfo::isAlive));
            if (isDbActive) {
                isDbActive =
                    DBChannelDispatcher.getHolders().get(dalgroup).getDbGroups().values().stream()
                        .filter(dbGroup -> dbGroup.getDbGroupUpdater().isMasterConfigured()).filter(
                        dbGroup -> !WeakMasterFilter.isWeakMasterDBGroup(dbGroup.getGroupName()))
                        .allMatch(DBGroup::masterElected);
            }
            isSuccess.set(isDbActive);
        });
        return isSuccess.get();
    }

    private FullHttpResponse buildAegisHCResp(String dalGroupId, boolean isSuccess, String msg)
        throws UnsupportedEncodingException {
        String json =
            "{\n" + "  \"success\":\"" + isSuccess + "\", \n" + "  \"msg\":\"" + msg + "\", \n"
                + "  \"data\": {\n" + "    \"isHealthy\":" + isSuccess + ", \n"
                + "    \"appid\": \"" + Constants.APPID + "\",\n" + "    \"host\" : \""
                + Constants.HOSTNAME + "\",\n" + "    \"dalGroupId\" : \"" + dalGroupId + "\"\n"
                + "    } " + "}";
        return buildResponse(json);
    }

    private String buildNotifyUrl() {
        String url = AthenaConfig.getInstance().getAegisHttpUrl();
        if (!url.endsWith("/")) {
            url = url + "/";
        }
        return url + "dal_group/deploy_result";
    }

    private boolean isOnline(DBConnectionInfo dbInfo) {
        boolean isOnline = DBChannelDispatcher.getHolders().values().stream().anyMatch(dalGroup -> {
            return dalGroup.isInScope(dbInfo.getGroup()) && !dalGroup.getZKCache().getOfflineDbs()
                .contains(dbInfo.getQualifiedDbId());
        });
        return isOnline;
    }

    private FullHttpResponse buildGlobalIdsResponse() throws UnsupportedEncodingException {
        try {
            String globalIdsJson = SafeJSONHelper.of(JacksonObjectMappers.getPrettyMapper())
                .writeValueAsStringOrDefault(GlobalId.allActiveGlobalIds(), "{}");
            FullHttpResponse response = buildResponse(globalIdsJson);
            response.headers().add(CONTENT_TYPE, Values.APPLICATION_JSON);
            return response;
        } catch (Exception e) {
            log.error("Exception in buildGlobalIdsResponse()", e);
            return buildResponse(e.toString());
        }
    }

    private FullHttpResponse handleSamples() throws UnsupportedEncodingException {
        try {
            String res = SafeJSONHelper.of(JacksonObjectMappers.getMapper())
                .writeValueAsStringOrDefault(DetectorDelegate.statistics(), "{}");
            FullHttpResponse response = buildResponse(res);
            response.headers().add(CONTENT_TYPE, Values.APPLICATION_JSON);
            return response;
        } catch (Exception e) {
            log.error(Objects.toString(e));
            return buildResponse(e.toString());
        }
    }


    private FullHttpResponse handleDefaultUrl() throws UnsupportedEncodingException {
        try {
            String res = SystemInfo.getInfo();
            return buildResponse(res);
        } catch (Exception e) {
            log.error(Objects.toString(e));
            return buildResponse(e.toString());
        }
    }

    private FullHttpResponse buildDalGroupHealthCheckResponse()
        throws UnsupportedEncodingException {
        try {
            String unhealthyDalGroupJson = SafeJSONHelper.of(JacksonObjectMappers.getPrettyMapper())
                .writeValueAsStringOrDefault(DetectorDelegate.statistics(), "{}");
            FullHttpResponse response = buildResponse(unhealthyDalGroupJson);
            response.headers().add(CONTENT_TYPE, "application/json; charset=UTF-8");
            return response;
        } catch (Exception e) {
            log.error(Objects.toString(e));
            return buildResponse(e.toString());
        }
    }
}
