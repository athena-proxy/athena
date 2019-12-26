package me.ele.jarch.athena.util.health.check;

import me.ele.jarch.athena.netty.AthenaServer;
import me.ele.jarch.athena.server.async.AsyncHeartBeat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Objects;

public class PGHealthCheckResponseHandler extends HealthCheckResponseHandler {
    private static final Logger logger =
        LoggerFactory.getLogger(PGHealthCheckResponseHandler.class);

    private volatile PGHealthCheckClient healthCheckClient = null;

    protected PGHealthCheckResponseHandler(ArrayList<HealthCheckItem> healthCheckItems) {
        super(healthCheckItems);
    }

    @Override protected AsyncHeartBeat getAsyncClient() {
        return healthCheckClient;
    }

    @Override protected void setClientSQL(String sql) {
        healthCheckClient.setSql(sql);
    }

    @Override protected void doRetry() {
        if (Objects.isNull(healthCheckClient)) {
            return;
        }
        AthenaServer.dalGroupHealthCheckJobScheduler
            .addOneTimeJob("dalGroup_healthcheck_retry", 100, () -> {
                logger.info("dalGroup health check retry, dalgroup is {}", currentItem.getTag());
                PGHealthCheckClient newHealthCheckClient =
                    new PGHealthCheckClient(healthCheckClient.getDbConnInfo(), this,
                        DalGroupHealthCheck.TIMEOUT);
                newHealthCheckClient.setSql((healthCheckClient).getSql());
                healthCheckClient = newHealthCheckClient;
                newHealthCheckClient.doAsyncExecute();
            });
    }

    public void setHealthCheckClient(PGHealthCheckClient healthCheckClient) {
        this.healthCheckClient = healthCheckClient;
    }
}
