package me.ele.jarch.athena.util.health.check;

import me.ele.jarch.athena.netty.AthenaServer;
import me.ele.jarch.athena.server.async.AsyncHeartBeat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Objects;

public class MysqlHealthCheckResponseHandler extends HealthCheckResponseHandler {
    private static final Logger logger =
        LoggerFactory.getLogger(MysqlHealthCheckResponseHandler.class);

    private volatile MysqlHealthCheckClient healthCheckClient = null;

    protected MysqlHealthCheckResponseHandler(ArrayList<HealthCheckItem> healthCheckItems) {
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
                MysqlHealthCheckClient newHCClient =
                    new MysqlHealthCheckClient(healthCheckClient.getDbConnInfo(), this,
                        DalGroupHealthCheck.TIMEOUT);
                newHCClient.setSql(healthCheckClient.getSql());
                this.healthCheckClient = newHCClient;
                newHCClient.doAsyncExecute();
            });
    }


    public void setHealthCheckClient(MysqlHealthCheckClient healthCheckClient) {
        this.healthCheckClient = healthCheckClient;
    }
}
