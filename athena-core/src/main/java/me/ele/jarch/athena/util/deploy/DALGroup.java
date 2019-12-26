package me.ele.jarch.athena.util.deploy;

import me.ele.jarch.athena.netty.LocalChannelServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class DALGroup {

    private static Logger logger = LoggerFactory.getLogger(DALGroup.class);

    private String name = "";
    private String org = "";
    private Boolean batchAllowed = false;
    private String dbGroup = "";
    private boolean pureSlaveOnly = false;
    private String slaveSelectStrategy = "default";
    private volatile boolean offline = false;

    public DALGroup(String name, String org, String dbGroup) {
        this.name = name;
        this.org = org;
        this.dbGroup = dbGroup;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getOrg() {
        return org;
    }

    public void setOrg(String org) {
        this.org = org;
    }

    public Boolean isBatchAllowed() {
        return batchAllowed;
    }

    public void setBatchAllowed(Boolean batchAllowed) {
        this.batchAllowed = batchAllowed;
        initLocalChannelServer(batchAllowed);
    }

    public boolean isPureSlaveOnly() {
        return pureSlaveOnly;
    }

    public void setPureSlaveOnly(boolean pureSlaveOnly) {
        this.pureSlaveOnly = pureSlaveOnly;
    }

    public String getSlaveSelectStrategy() {
        return slaveSelectStrategy;
    }

    public void setSlaveSelectStrategy(String slaveSelectStrategy) {
        this.slaveSelectStrategy = slaveSelectStrategy;
    }

    private void initLocalChannelServer(boolean isBatchSwitchAllowed) {
        try {
            if (!isBatchSwitchAllowed || LocalChannelServer.getInstance().isOpen()) {
                return;
            }
            LocalChannelServer.getInstance().start();
        } catch (Exception e) {
            logger.error("failed to start LocalChannelServer", e);
        }
    }

    public String getDbGroup() {
        return dbGroup;
    }

    public void setDbGroup(String dbGroup) {
        this.dbGroup = dbGroup;
    }

    public boolean isOffline() {
        return offline;
    }

    public void setOffline(boolean offline) {
        this.offline = offline;
    }

    @Override public int hashCode() {
        return Objects
            .hash(batchAllowed, dbGroup, name, offline, org, pureSlaveOnly, slaveSelectStrategy);
    }

    @Override public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        DALGroup other = (DALGroup) obj;
        return Objects.equals(batchAllowed, other.batchAllowed) && Objects
            .equals(dbGroup, other.dbGroup) && Objects.equals(name, other.name)
            && offline == other.offline && Objects.equals(org, other.org)
            && pureSlaveOnly == other.pureSlaveOnly && Objects
            .equals(slaveSelectStrategy, other.slaveSelectStrategy);
    }

    @Override public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("DALGroup [batchAllowed=");
        builder.append(batchAllowed);
        builder.append(", dbGroup=");
        builder.append(dbGroup);
        builder.append(", name=");
        builder.append(name);
        builder.append(", offline=");
        builder.append(offline);
        builder.append(", org=");
        builder.append(org);
        builder.append(", pureSlaveOnly=");
        builder.append(pureSlaveOnly);
        builder.append(", slaveSelectStrategy=");
        builder.append(slaveSelectStrategy);
        builder.append("]");
        return builder.toString();
    }

}
