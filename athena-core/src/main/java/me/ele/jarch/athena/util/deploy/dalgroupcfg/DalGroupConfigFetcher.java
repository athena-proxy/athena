package me.ele.jarch.athena.util.deploy.dalgroupcfg;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import me.ele.jarch.athena.constant.Metrics;
import me.ele.jarch.athena.constant.TraceNames;
import me.ele.jarch.athena.netty.AthenaHttpAsyncClient;
import me.ele.jarch.athena.netty.AthenaServer;
import me.ele.jarch.athena.util.AthenaConfig;
import me.ele.jarch.athena.util.etrace.MetricFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DalGroupConfigFetcher {
    private static Logger logger = LoggerFactory.getLogger(DalGroupConfigFetcher.class);

    public static void fetchDalGroupCfg(AegisRquest fetchRequest) {
        logger.info("Start to fetch dalgroup config from athena_admin for dalgroup ID {} ",
            fetchRequest.dalgroupId);
        HttpGet request = new HttpGet(buildQueryUrl(fetchRequest));
        AthenaHttpAsyncClient.getHttpClient().execute(request, new FutureCallback<HttpResponse>() {
            @Override public void completed(HttpResponse result) {
                try {
                    String content = EntityUtils.toString(result.getEntity(), "UTF-8");
                    logger
                        .info("dalgroup config for dalgroup ID {} is : {}", fetchRequest.dalgroupId,
                            content);
                    ObjectMapper objectMapper = new ObjectMapper()
                        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
                    DalGroupConfigResponse response =
                        objectMapper.readValue(content, DalGroupConfigResponse.class);
                    if (response.getSuccess()) {
                        response.getDalGroupConfig().write2File();
                    } else {
                        logger.error(
                            "Error fetching dalgroup config from Aegis, dalgroup {}, error message {}",
                            fetchRequest.dalgroupId, response.getMsg());
                        retryFetchDalGroupCfg(fetchRequest);
                    }
                } catch (Exception e) {
                    logger.error("Exception while reading the response ", e);
                    retryFetchDalGroupCfg(fetchRequest);
                }
            }

            @Override public void failed(Exception ex) {
                logger.error("Error fetching dalgroup config from Aegis, dalgroup {}",
                    fetchRequest.dalgroupId, ex);
                retryFetchDalGroupCfg(fetchRequest);
            }

            @Override public void cancelled() {
                logger.error(
                    "Job canceled  when try fetching dalgroup config from Aegis, dalgroup {}",
                    fetchRequest.dalgroupId);
            }
        });
    }

    private static void retryFetchDalGroupCfg(AegisRquest aegisRquest) {
        if (aegisRquest.retryCount.decrementAndGet() > -1) {
            AthenaServer.quickJobScheduler
                .addOneTimeJob("Aegis_retry_job", aegisRquest.getDelayMilli(), () -> {
                    logger.info("Retrying fetching dalgroup config from Aegis for dalgroup id {}",
                        aegisRquest.dalgroupId);
                    DalGroupConfigFetcher.fetchDalGroupCfg(aegisRquest);
                });
        } else {
            MetricFactory.newCounter(Metrics.DALGROUP_CONFIG_LOAD)
                .addTag(TraceNames.DALGROUP, aegisRquest.dalgroup)
                .addTag(TraceNames.STATUS, "Failed").once();
        }
    }

    private static String buildQueryUrl(AegisRquest fetchRequest) {
        String url = StringUtils.isEmpty(fetchRequest.aegisUrl) ?
            AthenaConfig.getInstance().getAegisHttpUrl() :
            fetchRequest.aegisUrl;
        if (!url.endsWith("/")) {
            url = url + "/";
        }
        return url + "dal_group/query?id=" + fetchRequest.dalgroupId;
    }
}
