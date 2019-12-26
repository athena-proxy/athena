package me.ele.jarch.athena.allinone;

import com.fasterxml.jackson.annotation.JsonCreator;

public enum DBVendor {
    MYSQL, PG;

    public static DBVendor getVendor(String name) {
        for (DBVendor vendor : values()) {
            if (vendor.name().equalsIgnoreCase(name)) {
                return vendor;
            }
        }
        return MYSQL;
    }

    @JsonCreator public static DBVendor fromString(String key) {
        return key == null ? null : DBVendor.valueOf(key.toUpperCase());
    }
}
