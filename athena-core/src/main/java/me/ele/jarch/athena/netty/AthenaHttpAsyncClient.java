package me.ele.jarch.athena.netty;

import me.ele.jarch.athena.util.AthenaConfig;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class AthenaHttpAsyncClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(AthenaHttpAsyncClient.class);
    private static final int TIME_OUT = 10 * 1000;
    private static final int MAX_CONN = 10;
    private static CloseableHttpAsyncClient httpClient = null;
    private final static Object syncLock = new Object();
    private static int MAX_THREAD = 2;

    static {
        String maxThread = AthenaConfig.getInstance().getHttpAsyncClientMaxThread();
        MAX_THREAD = Integer.valueOf(maxThread);
    }

    public static CloseableHttpAsyncClient getHttpClient() {
        if (httpClient == null) {
            synchronized (syncLock) {
                if (httpClient == null) {
                    httpClient = createHttpClient();
                }
            }
        }
        return httpClient;
    }

    public static CloseableHttpAsyncClient createHttpClient() {
        RequestConfig requestConfig =
            RequestConfig.custom().setConnectionRequestTimeout(TIME_OUT).setConnectTimeout(TIME_OUT)
                .setSocketTimeout(TIME_OUT).build();
        IOReactorConfig ioReactorConfig =
            IOReactorConfig.custom().setIoThreadCount(MAX_THREAD).setSoKeepAlive(true).build();

        CloseableHttpAsyncClient httpClient =
            HttpAsyncClients.custom().setDefaultRequestConfig(requestConfig).
                setMaxConnTotal(MAX_CONN).setMaxConnPerRoute(MAX_CONN)
                .setDefaultIOReactorConfig(ioReactorConfig).build();
        httpClient.start();
        return httpClient;
    }

    public static void sendHttpGet(String requestUrl, Map<String, String> paramsMap)
        throws IOException {
        String httpUrl = requestUrl;
        if (paramsMap != null && paramsMap.size() > 0) {
            //append params string on url
            List<NameValuePair> nameValuePairs = new ArrayList<>();
            for (String key : paramsMap.keySet()) {
                nameValuePairs.add(new BasicNameValuePair(key, paramsMap.get(key)));
            }
            String paramString =
                EntityUtils.toString(new UrlEncodedFormEntity(nameValuePairs, "UTF-8"));
            httpUrl = httpUrl + "?" + paramString;
        }
        HttpGet httpGet = new HttpGet(httpUrl);
        sendHttpPostGet(httpGet);
    }

    public static void sendHttpPost(String httpUrl, Map<String, String> paramsMap)
        throws IOException {
        HttpPost httpPost = new HttpPost(httpUrl);
        if (paramsMap != null && paramsMap.size() > 0) {
            List<NameValuePair> nameValuePairs = new ArrayList<>();
            for (String key : paramsMap.keySet()) {
                nameValuePairs.add(new BasicNameValuePair(key, paramsMap.get(key)));
            }
            httpPost.setEntity(new UrlEncodedFormEntity(nameValuePairs, "UTF-8"));
        }
        sendHttpPostGet(httpPost);
    }

    private static void sendHttpPostGet(HttpRequestBase httpRequest) throws IOException {
        CloseableHttpAsyncClient httpClient = getHttpClient();
        if (httpClient != null) {
            httpClient.execute(httpRequest, HttpClientContext.create(),
                new FutureCallback<HttpResponse>() {
                    @Override public void completed(HttpResponse httpResponse) {
                        try {
                            String content =
                                EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
                            if (content != null
                                && httpResponse.getStatusLine().getStatusCode() == 200) {
                                LOGGER.debug("Async http request success, response :" + (
                                    content.length() > 50 ? content.substring(0, 50) : content));
                            } else {
                                LOGGER.error("Async http request error, response" + content);
                            }
                        } catch (IOException e) {
                            LOGGER.error("Parse response error.", e);
                        }
                    }

                    @Override public void failed(Exception e) {
                        LOGGER.error(Objects.toString(e));
                    }

                    @Override public void cancelled() {
                        LOGGER.error("Async http cancelled.");
                    }
                });
        }
    }
}
