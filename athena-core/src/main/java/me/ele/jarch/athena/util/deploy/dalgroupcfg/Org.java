package me.ele.jarch.athena.util.deploy.dalgroupcfg;

import com.fasterxml.jackson.annotation.JsonIgnore;
import me.ele.jarch.athena.constant.Constants;

import java.util.*;
import java.util.stream.Collectors;

public class Org {

    private String name;
    private Integer port;
    @JsonIgnore private volatile List<String> deployedHosts = new ArrayList<>(2);
    @JsonIgnore private Map<String, Object> additionalProperties = new HashMap<String, Object>();
    private Map<String, String> deploy = new HashMap<>();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public List<String> getDeployedHosts() {
        return deployedHosts;
    }

    public void setDeployedHosts(List<String> deploy) {
        this.deployedHosts = deploy;
    }

    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

    public Map<String, String> getDeploy() {
        return deploy;
    }

    public void setDeploy(Map<String, String> deploy) {
        this.deploy = deploy;
        configureHosts();
    }

    private void configureHosts() {
        String curHosts = this.deploy.get(Constants.APPID);
        if (curHosts != null && curHosts.trim().length() != 0) {
            deployedHosts = Arrays.stream(curHosts.trim().split(",")).map(String::trim)
                .collect(Collectors.toList());
        }
    }

    public boolean shouldLoad() {
        return deployedHosts.contains(Constants.HOSTNAME);
    }
}
