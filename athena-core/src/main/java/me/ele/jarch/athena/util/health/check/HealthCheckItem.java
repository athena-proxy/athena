package me.ele.jarch.athena.util.health.check;

public class HealthCheckItem {
    private String tag;
    private String sql;
    private DalGroupHealthCheckStatus failStatus;

    public HealthCheckItem(String tag, String sql, DalGroupHealthCheckStatus failStatus) {
        this.tag = tag;
        this.sql = sql;
        this.failStatus = failStatus;
    }

    public String getTag() {
        return tag;
    }

    public String getSql() {
        return sql;
    }


    public DalGroupHealthCheckStatus getFailStatus() {
        return failStatus;
    }
}
