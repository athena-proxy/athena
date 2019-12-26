package me.ele.jarch.athena.server.async;

import me.ele.jarch.athena.allinone.DBConnectionInfo;
import me.ele.jarch.athena.constant.Metrics;
import me.ele.jarch.athena.sql.ResultSet;
import me.ele.jarch.athena.util.GreySwitch;
import me.ele.jarch.athena.util.etrace.MetricFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MysqlAsyncHeartBeat extends AsyncHeartBeat {
    private static Logger LOGGER = LoggerFactory.getLogger(MysqlAsyncHeartBeat.class);
    protected final AsyncResultSetHandler resultHandler;

    // 有result ->slave，null->master
    private static String SLAVE_STATUS_QUERY = "SHOW SLAVE STATUS";

    //判断主从，true->slave false->master 优先上面
    private static String READY_ONLY_QUERY = "SELECT @@global.read_only";

    private volatile String currentQuery = SLAVE_STATUS_QUERY;
    private volatile boolean slaveFlag = false;

    protected MysqlAsyncHeartBeat(DBConnectionInfo dbConnInfo,
        AsyncResultSetHandler resultHandler) {
        super(dbConnInfo, resultHandler);
        bindQuerySql4AllMaster();
        this.resultHandler = resultHandler;
    }

    private void bindQuerySql4AllMaster() {
        if (!this.dbConnInfo.getDistMatser()) {
            currentQuery = READY_ONLY_QUERY;
        }
    }

    @Override public boolean hasSlaveFlag() {
        return slaveFlag;
    }

    public void setSlaveFalg(ResultSet rs) {
        if (rs.getErr() != null) {
            this.slaveFlag = false;
            sendShowSLaveFailTrace();
            return;
        }
        if (rs.getRowCount() == 0) {
            this.slaveFlag = false;
            return;
        }
        this.slaveFlag = true;
    }

    @Override protected String getQuery() {
        if (dbConnInfo.getDistMatser()) {
            return currentQuery;
        }
        return READY_ONLY_QUERY;
    }

    @Override protected void handleResultSet(ResultSet rs) {
        if (this.currentQuery.equals(READY_ONLY_QUERY)) {
            if (dbConnInfo.getDistMatser()) {
                this.currentQuery = SLAVE_STATUS_QUERY;
            }
            resultHandler.handleResultSet(rs, true, "sucess");
            return;
        }

        if (this.currentQuery.equals(SLAVE_STATUS_QUERY)) {
            this.currentQuery = READY_ONLY_QUERY;
            this.setSlaveFalg(rs);
            this.execute();
            return;
        }
        LOGGER.error("MysqlAsyncHeartBeat error, unknown currentQuery " + currentQuery);
    }

    private void sendShowSLaveFailTrace() {
        if (!GreySwitch.getInstance().isMhaSlaveFailedEnabed()) {
            return;
        }
        LOGGER
            .error("Get slave status failed, DBGroup is '{}', DBId is '{}'", dbConnInfo.getGroup(),
                dbConnInfo.getId());
        MetricFactory.newCounterWithDBConnectionInfo(Metrics.SHOW_SLAVE_FAILED, dbConnInfo).once();
    }
}
