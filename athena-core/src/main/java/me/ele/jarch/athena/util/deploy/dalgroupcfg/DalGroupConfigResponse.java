package me.ele.jarch.athena.util.deploy.dalgroupcfg;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DalGroupConfigResponse {

    private boolean success;

    private String code;

    private String msg;
    @JsonProperty("data") private DalGroupConfig dalGroupConfig;

    public boolean getSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public DalGroupConfig getDalGroupConfig() {
        return dalGroupConfig;
    }

    public void setDalGroupConfig(DalGroupConfig dalGroupConfig) {
        this.dalGroupConfig = dalGroupConfig;
    }
}
