package me.ele.jarch.athena.util.deploy.dalgroupcfg;

import com.fasterxml.jackson.annotation.JsonCreator;

public enum MlType {
    //非多活
    NOT_ML, //多活
    ML, // global zone
    GZ, //others
    UNKNOWN;

    @JsonCreator public static MlType fromString(String key) {
        return key == null ? null : MlType.valueOf(key.toUpperCase());
    }
}
