package me.ele.jarch.athena.util.health.check;

import me.ele.jarch.athena.server.async.AsyncHeartBeat;
import me.ele.jarch.athena.server.async.AsyncResultSetHandler;
import me.ele.jarch.athena.sql.ResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class HealthCheckResponseHandler implements AsyncResultSetHandler {
    private static final Logger logger = LoggerFactory.getLogger(HealthCheckResponseHandler.class);

    protected final AtomicInteger retry = new AtomicInteger(0);

    protected long birthDay = System.currentTimeMillis();

    protected Iterator<HealthCheckItem> iterator;
    protected HealthCheckItem currentItem;

    public HealthCheckResponseHandler(ArrayList<HealthCheckItem> healthCheckItems) {
        this.iterator = healthCheckItems.iterator();
        this.currentItem = this.iterator.next();
    }

    protected abstract AsyncHeartBeat getAsyncClient();

    protected abstract void setClientSQL(String sql);

    @Override public void handleResultSet(ResultSet rs, boolean isDone, String reason) {
        try {
            if (Objects.nonNull(rs) && rs.errorHappened()) {
                logger.error(
                    "Got an error when do dalgroup health check, dalgroup is {}, error is {}",
                    currentItem.getTag(), rs.getErr().errorMessage);
            }
            boolean success = isDone && Objects.nonNull(rs) && !rs.errorHappened();
            if (!success && retry.getAndIncrement() < DalGroupHealthCheck.RETRY_COUNT) {
                doRetry();
                closeConn();
            } else {
                runNextSqlOrCloseConn(success);
            }
        } catch (Exception e) {
            logger.error(
                "Exception in dalGroup health check asyncResultSet Handler " + currentItem.getTag(),
                e);
            closeConn();
        }
    }

    private void runNextSqlOrCloseConn(boolean success) {
        DalGroupHealthCheckStatus status =
            success ? DalGroupHealthCheckStatus.HEART_BEAT_SUCCESS : currentItem.getFailStatus();
        DalGroupHealthCheck.HEALTH_CHECK_RESULT.put(currentItem.getTag(), status);
        logger.info("dalgroup health check finished, dalgroup is {}, status: {}, time used {}",
            currentItem.getTag(), status, System.currentTimeMillis() - birthDay);
        if (iterator.hasNext()) {
            runNextSql();
        } else {
            closeConn();
        }
    }

    protected void runNextSql() {
        birthDay = System.currentTimeMillis();
        retry.getAndSet(0);
        currentItem = iterator.next();
        String sql = currentItem.getSql();
        setClientSQL(sql);
        if (!getAsyncClient().doAsyncExecute()) {
            logger.error("dalgroup health check doAsyncExecute error, dalgroup is {}",
                currentItem.getTag());
            closeConn();
        }
    }


    protected void closeConn() {
        AsyncHeartBeat asyncClient = getAsyncClient();
        if (Objects.isNull(asyncClient)) {
            return;
        }
        asyncClient.setActive(false);
        asyncClient.doQuit("quit in dalgroup HealthCheckResponseHandler");
    }


    abstract void doRetry();

}
