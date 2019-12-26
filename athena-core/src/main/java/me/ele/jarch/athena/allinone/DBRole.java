package me.ele.jarch.athena.allinone;

import com.fasterxml.jackson.annotation.JsonCreator;

public enum DBRole {
    MASTER, SLAVE, DUMMY, GRAY;

    @JsonCreator public static DBRole fromString(String key) {
        return key == null ? null : DBRole.valueOf(key.toUpperCase());
    }
}
