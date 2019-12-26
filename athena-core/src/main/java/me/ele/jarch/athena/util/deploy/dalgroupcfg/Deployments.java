package me.ele.jarch.athena.util.deploy.dalgroupcfg;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Collections;
import java.util.List;

public class Deployments {

    private String id;

    private String aegisUrl;

    private DeployMode deployMode;

    private List<Deploy> verifyDeploy;

    private List<Deploy> greyDeploy;

    private List<Deploy> stableDeploy;

    public String getId() {
        return id;
    }

    public DeployMode getDeployMode() {
        return deployMode;
    }

    public String getAegisUrl() {
        return aegisUrl;
    }

    public void setAegisUrl(String aegisUrl) {
        this.aegisUrl = aegisUrl;
    }

    public void setDeployMode(DeployMode deployMode) {
        this.deployMode = deployMode;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<Deploy> getVerifyDeploy() {
        return verifyDeploy;
    }

    public void setVerifyDeploy(List<Deploy> verifyDeploy) {
        this.verifyDeploy = verifyDeploy;
    }

    public List<Deploy> getGreyDeploy() {
        return greyDeploy;
    }

    public void setGreyDeploy(List<Deploy> greyDeploy) {
        this.greyDeploy = greyDeploy;
    }

    public List<Deploy> getStableDeploy() {
        return stableDeploy;
    }

    public void setStableDeploy(List<Deploy> stableDeploy) {
        this.stableDeploy = stableDeploy;
    }

    public List<Deploy> getDeploy() {
        switch (deployMode) {
            case GREY:
                return this.greyDeploy;
            case RELEASE:
                return this.stableDeploy;
            case VERIFY:
                return this.verifyDeploy;
            default:
                return Collections.emptyList();
        }
    }

    public enum DeployMode {
        //校验模式
        VERIFY, //灰度模式
        GREY, //上线模式
        RELEASE;

        @JsonCreator public static DeployMode fromString(String key) {
            return key == null ? null : DeployMode.valueOf(key.toUpperCase());
        }

    }
}
