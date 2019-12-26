package me.ele.jarch.athena.allinone;

import java.util.concurrent.atomic.AtomicInteger;

public class HeartBeatResult {
    private String group;
    private String id;
    private Boolean alive;
    private Boolean readOnly;
    final private AtomicInteger failedTimes = new AtomicInteger(0);
    private String qualifiedDbId;
    private DBRole role;
    private boolean slaveFlag;

    public HeartBeatResult(String group, String id, Boolean alive, Boolean readOnly,
        int failedTimes, DBRole role) {
        this(group, id, alive, readOnly, failedTimes, role, group + ":" + id);
    }

    public HeartBeatResult(String group, String id, Boolean alive, Boolean readOnly,
        int failedTimes, DBRole role, String qualifiedDbId) {
        this(group, id, alive, readOnly, failedTimes, role, qualifiedDbId, false);
    }

    public HeartBeatResult(String group, String id, Boolean alive, Boolean readOnly,
        int failedTimes, DBRole role, String qualifiedDbId, boolean slaveFlag) {
        super();
        this.group = group;
        this.id = id;
        this.alive = alive;
        this.readOnly = readOnly;
        this.failedTimes.set(failedTimes);
        this.qualifiedDbId = qualifiedDbId;
        this.role = role;
        this.slaveFlag = slaveFlag;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Boolean isAlive() {
        return alive;
    }

    public void setAlive(Boolean alive) {
        this.alive = alive;
    }

    public Boolean isReadOnly() {
        return readOnly;
    }

    public void setReadOnly(Boolean readOnly) {
        this.readOnly = readOnly;
    }

    public int getFailedTimes() {
        return failedTimes.get();
    }

    public int setFailedTimes(int failedTimes) {
        return this.failedTimes.getAndSet(failedTimes);
    }

    public int incrementAndGetFailedTimes() {
        return this.failedTimes.incrementAndGet();
    }

    @Override public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((alive == null) ? 0 : alive.hashCode());
        result = prime * result + ((group == null) ? 0 : group.hashCode());
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((readOnly == null) ? 0 : readOnly.hashCode());
        return result;
    }

    @Override public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        HeartBeatResult other = (HeartBeatResult) obj;
        if (alive == null) {
            if (other.alive != null)
                return false;
        } else if (!alive.equals(other.alive))
            return false;
        if (failedTimes != other.failedTimes)
            return false;
        if (group == null) {
            if (other.group != null)
                return false;
        } else if (!group.equals(other.group))
            return false;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        if (readOnly == null) {
            if (other.readOnly != null)
                return false;
        } else if (!readOnly.equals(other.readOnly))
            return false;
        return true;
    }

    public String getQualifiedDbId() {
        return this.qualifiedDbId;
    }

    public DBRole getRole() {
        return role;
    }

    public void setRole(DBRole role) {
        this.role = role;
    }

    public boolean isSlaveFlag() {
        return slaveFlag;
    }

    public void setSlaveFlag(boolean slaveFlag) {
        this.slaveFlag = slaveFlag;
    }

    @Override public String toString() {
        return "HeartBeatResult [group=" + group + ", id=" + id + ", alive=" + alive + ", readOnly="
            + readOnly + ", failedTimes=" + failedTimes + ", qualifiedDbId=" + qualifiedDbId
            + ", role=" + role + ", slaveFlag=" + slaveFlag + "]";
    }

}
