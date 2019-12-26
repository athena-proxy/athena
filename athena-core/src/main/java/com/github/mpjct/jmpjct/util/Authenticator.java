package com.github.mpjct.jmpjct.util;

import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.exception.AuthQuitState;
import me.ele.jarch.athena.scheduler.DBChannelDispatcher;
import me.ele.jarch.athena.util.AthenaConfig;
import me.ele.jarch.athena.util.CredentialsCfg;
import me.ele.jarch.athena.util.UserInfo;
import me.ele.jarch.athena.util.deploy.dalgroupcfg.DalGroupConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Objects;

public class Authenticator {

    private static final Logger logger = LoggerFactory.getLogger(Authenticator.class);

    protected volatile byte[] authSeed = new byte[20];// 20 bit
    public String userName = "unknown_user";
    private byte[] passwordAfterEncryption;
    protected String schema = "";
    private boolean isLegalSchema = true;
    public volatile boolean isAuthenticated = false;
    public volatile String clientAddr = "1.1.1.1";
    private boolean readOnly = false;
    private StringBuilder authFailedReason = new StringBuilder();
    public volatile boolean isDalHealthCheckUser;
    public final long birthdayInMill = System.currentTimeMillis();
    private AuthQuitState state = AuthQuitState.OTHER;

    public AuthQuitState getState() {
        return state;
    }

    public void setState(AuthQuitState state) {
        this.state = state;
    }

    /**
     * @param seed           first 8bit
     * @param restOfScramble second 13bit 0 terminated
     * @return
     */
    public void setAutoSeed(final byte[] seed, final byte[] restOfScramble) {

        System.arraycopy(seed, 0, authSeed, 0, seed.length);
        System.arraycopy(restOfScramble, 0, authSeed, seed.length, restOfScramble.length - 1);
    }

    public void setUserAndPassWord(String userName, byte[] passwordAfterEncryption, String schema) {
        this.userName = userName;
        this.passwordAfterEncryption = passwordAfterEncryption;
        this.schema = schema == null ? "" : schema;
        this.isDalHealthCheckUser = Constants.DAL_HEARTBEAT.equals(userName);
    }

    public void setAddress(String clientAddr) {
        this.clientAddr = clientAddr;
    }

    public String getClientIp() {
        String[] items = clientAddr.split(":");
        String ip = "";
        if (items.length > 0 && items[0].trim().length() > 1) {
            ip = items[0].trim().substring(1);
        }
        return ip;
    }

    /**
     * 用户名密码校验分2步：先校验 {@link DalGroupConfig} 中的用户名密码，若校验通过则返回，反之则继续在 {@link CredentialsCfg}配置中校验。
     *
     * @return
     */
    public boolean initAndVerifyUser() {
        if (validateByDalgroupCfg()) {
            return true;
        }
        String userNameWithChannel =
            new StringBuilder(64).append(userName).append('@').append(schema).toString();
        UserInfo userInfo = CredentialsCfg.getConfig().get(userNameWithChannel);
        if (userInfo == null) {
            this.isLegalSchema = false;
            userInfo = CredentialsCfg.getConfig().get(userName);
        }
        if (userInfo == null) {
            logger.error(String.format("user %s with schema %s does not exist", userName, schema));
            return false;
        }

        this.readOnly = userInfo.isReadOnly();

        return validatePassword(userInfo.getEncryptedPwd());
    }

    /**
     * @param serverPasswd 系统配置密码
     * @return
     */
    protected boolean validatePassword(String serverPasswd) {
        final byte[] mysql_user_pass = ByteUtil.hexStringToByte(serverPasswd);

        byte[] shouldBeMysql_user_password;
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            md.update(authSeed);
            byte[] pass4 = md.digest(mysql_user_pass);// SHA1(scramble + mysql.user.Password)
            byte[] shouldBeStage1_hash = new byte[pass4.length];
            for (int i = 0; i < pass4.length; i++) {
                shouldBeStage1_hash[i] = (byte) (pass4[i]
                    ^ passwordAfterEncryption[i]);// token XOR SHA1(scramble + mysql.user.Password)
            }
            md.reset();
            shouldBeMysql_user_password = md.digest(shouldBeStage1_hash);
        } catch (NoSuchAlgorithmException t) {
            logger.error("NoSuchAlgorithmException caught!", t);
            return false;
        }
        this.isAuthenticated = Arrays.equals(shouldBeMysql_user_password, mysql_user_pass);
        return this.isAuthenticated;
    }

    protected boolean validateByDalgroupCfg() {
        DBChannelDispatcher dispatcher = DBChannelDispatcher.getHolders().get(schema);
        if (Objects.isNull(dispatcher) || DalGroupConfig.DUMMY
            .equals(dispatcher.getDalGroupConfig())) {
            return false;
        }
        if (!dispatcher.getDalGroupConfig().getUsername().equals(userName)) {
            return false;
        }
        if (!validatePassword(dispatcher.getDalGroupConfig().getPassword())) {
            return false;
        }
        this.readOnly = dispatcher.getDalGroupConfig().getReadonlyUser();
        return true;
    }

    public boolean checkPrivileges() {
        if (userName.equals(Constants.ATHENA_ADMIN_READONLY) && !AthenaConfig.getInstance()
            .getLoopbackIP().equals(getClientIp())) {
            return false;
        }
        return true;
    }

    @Override public String toString() {
        String errorMsg = "Access denied for user '" + userName + "'@'" + clientAddr + "'";
        return errorMsg;
    }

    public String getSchema() {
        return schema;
    }

    public boolean isLegalSchema() {
        return isLegalSchema;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public String getAuthFailedReason() {
        return authFailedReason.toString();
    }

    public void addAuthFailedReason(String reason4AuthFail) {
        this.authFailedReason.append(reason4AuthFail);
    }
}
