package me.ele.jarch.athena.util.deploy.dalgroupcfg;

import java.util.List;

public class Deploy {
    private String appid;

    private List<String> hostnames;

    public String getAppid() {
        return appid;
    }

    public void setAppid(String appid) {
        this.appid = appid;
    }

    public List<String> getHostnames() {
        return hostnames;
    }

    public void setHostnames(List<String> hostnames) {
        this.hostnames = hostnames;
    }
}
