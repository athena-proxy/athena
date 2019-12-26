package me.ele.jarch.athena.allinone;

import java.util.Objects;

public class DBStatus {
    private String group;
    private String id;
    private boolean active = true;


    public DBStatus(String group, String id, boolean active) {
        super();
        this.group = group;
        this.id = id;
        this.active = active;
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

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    @Override public String toString() {
        return "DBStatus [group=" + group + ", id=" + id + ", active=" + active + "]";
    }

    @Override public int hashCode() {
        return Objects.hash(active, group, id);
    }

    @Override public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || !(obj instanceof DBStatus)) {
            return false;
        }

        DBStatus other = (DBStatus) obj;
        return (Objects.equals(active, other.active) && Objects.equals(group, other.group)
            && Objects.equals(id, other.id));
    }

    public String getQualifiedDbId() {
        return this.group + ":" + id;
    }

}
