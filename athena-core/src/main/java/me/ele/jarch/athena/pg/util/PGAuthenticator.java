package me.ele.jarch.athena.pg.util;

import com.github.mpjct.jmpjct.util.Authenticator;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.pg.proto.ErrorResponse;
import me.ele.jarch.athena.util.CredentialsCfg;
import me.ele.jarch.athena.util.UserInfo;
import me.ele.jarch.athena.util.deploy.dalgroupcfg.DalGroupConfig;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public class PGAuthenticator extends Authenticator {
    private ErrorResponse errorResponse = null;
    private Map<String, String> clientAttributes = Collections.emptyMap();
    private volatile String clientPasswd = "";


    public ErrorResponse getErrorResponse() {
        return errorResponse;
    }

    public void setErrorResponse(ErrorResponse errorResponse) {
        this.errorResponse = errorResponse;
    }

    public void setUserName(String userName) {
        if (StringUtils.isEmpty(userName)) {
            return;
        }

        this.userName = userName;
        this.isDalHealthCheckUser = Constants.DAL_HEARTBEAT.equals(userName);
    }

    public void setSchema(String schema) {
        if (StringUtils.isNotEmpty(schema)) {
            this.schema = schema;
        }
    }

    public byte[] getAuthSeed() {
        return authSeed;
    }

    public void setAuthSeed(byte[] salt) {
        this.authSeed = salt;
    }

    /**
     * 用户名密码校验分2步：先校验 {@link DalGroupConfig} 中的用户名密码，若校验通过则返回，反之则继续在 {@link CredentialsCfg}配置中校验。
     *
     * @return
     */
    public boolean checkPassword(String password) {
        if (StringUtils.isEmpty(userName) || StringUtils.isEmpty(password) || Objects
            .isNull(authSeed)) {
            return false;
        }
        this.clientPasswd = password;
        if (validateByDalgroupCfg()) {
            return true;
        }
        String literalName = userName + "@" + schema;
        /**如果用户名是athena_admin或athena_admin_readonly,则替换schema为*构造literalName*/
        if (Constants.ATHENA_ADMIN.equals(userName) || Constants.ATHENA_ADMIN_READONLY
            .equals(userName)) {
            literalName = userName + "@*";
        }
        UserInfo userInfo = CredentialsCfg.getConfig().get(literalName);
        if (userInfo == null) {
            addAuthFailedReason(String.format("no literal user %s found ", literalName));
            return false;
        }
        String realPwd = userInfo.getEncryptedPwd();
        if (StringUtils.isEmpty(realPwd)) {
            return true;
        }
        return validatePassword(realPwd);
    }

    @Override protected boolean validatePassword(String serverPasswd) {
        byte[] realPwdMD5 =
            MD5Digest.encode(serverPasswd.toLowerCase().getBytes(StandardCharsets.UTF_8), authSeed);
        byte[] pwdMD5 = clientPasswd.toLowerCase().getBytes(StandardCharsets.UTF_8);
        return Arrays.equals(realPwdMD5, pwdMD5);
    }

    public Map<String, String> getClientAttributes() {
        return clientAttributes;
    }

    public void setClientAttributes(Map<String, String> clientAttributes) {
        this.clientAttributes = clientAttributes;
    }
}
