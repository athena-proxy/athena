package me.ele.jarch.athena.sql;

import com.github.mpjct.jmpjct.mysql.proto.ERR;
import com.github.mpjct.jmpjct.mysql.proto.OK;
import com.github.mpjct.jmpjct.util.Authenticator;
import com.github.mpjct.jmpjct.util.ErrorCode;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.exception.AuthQuitState;
import me.ele.jarch.athena.exception.QuitException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CmdAuthResultFake extends OnePacketCommand {

    private static final Logger logger = LoggerFactory.getLogger(CmdAuthResultFake.class);
    private Authenticator authenticator;

    public CmdAuthResultFake(Authenticator authenticator) {
        super(new byte[0]);
        this.authenticator = authenticator;
    }

    @Override public boolean doInnerExecute() throws QuitException {

        if (Constants.DAL_HEARTBEAT.equals(authenticator.userName)) {
            authenticator.isAuthenticated = true;
            sendBuf = OK.getDefaultOkPacketBytes();
            return true;
        }
        // 校验用户特权,如果用户是athena_admin_readonly超级用户,限制loopBackIp登录
        if (!authenticator.checkPrivileges()) {
            authenticator.isAuthenticated = false;
            buildError();
            String errorUserName = "Invalid host.(host: '" + authenticator.getClientIp() + "') ";
            authenticator.addAuthFailedReason(errorUserName);
            logger.error(errorUserName);
            return true;
        }

        if (!authenticator.initAndVerifyUser()) {
            buildError();
            String errorUserName =
                "Invalid user name or password.(input username: '" + authenticator.userName + "') ";
            authenticator.setState(AuthQuitState.ERROR_PASSWORD);
            authenticator.addAuthFailedReason(errorUserName);
            logger.error(errorUserName);
            return true;
        }

        sendBuf = OK.getDefaultOkPacketBytes();
        return true;
    }

    public void buildError() {
        sendBuf = ERR.buildErr(2, ErrorCode.ER_ACCESS_DENIED_ERROR.getErrorNo(), "28000",
            authenticator.toString()).toPacket();
        authenticator.isAuthenticated = false;
    }

    public void buildSchemaError() {
        sendBuf = ERR.buildErr(2, ErrorCode.ER_ACCESS_DENIED_ERROR.getErrorNo(), "28000",
            "Empty or Illegal schema for user: " + authenticator.userName).toPacket();
        authenticator.isAuthenticated = false;
    }

    public void buildDalHeartBeatError() {
        sendBuf = ERR.buildErr(2, ErrorCode.ER_SERVER_SHUTDOWN.getErrorNo(), "08S01",
            "Server shutdown in progress").toPacket();
    }
}
