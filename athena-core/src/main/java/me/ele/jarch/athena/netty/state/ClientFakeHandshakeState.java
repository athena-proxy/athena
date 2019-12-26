package me.ele.jarch.athena.netty.state;

import io.netty.buffer.Unpooled;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.sql.CmdHandshakeFake;

public class ClientFakeHandshakeState implements State {

    protected SqlSessionContext sqlSessionContext;

    public ClientFakeHandshakeState(SqlSessionContext sqlSessionContext) {
        this.sqlSessionContext = sqlSessionContext;
    }

    @Override public boolean handle() throws QuitException {
        return doClientHandshake();
    }

    @Override public SESSION_STATUS getStatus() {
        return SESSION_STATUS.CLIENT_FAKE_HANDSHAKE;
    }

    private boolean doClientHandshake() throws QuitException {
        sqlSessionContext.authenticator.setAddress(sqlSessionContext.getClientAddr());
        CmdHandshakeFake cmdHandshake = new CmdHandshakeFake(sqlSessionContext.connectionId);
        cmdHandshake.execute();
        sqlSessionContext.authenticator
            .setAutoSeed(cmdHandshake.handshake.challenge1, cmdHandshake.handshake.challenge2);
        // setStatus(SESSION_STATUS.CLIENT_FAKE_AUTH);
        sqlSessionContext.setState(SESSION_STATUS.CLIENT_FAKE_AUTH);
        //start trace login process
        sqlSessionContext.sqlSessionContextUtil.startLoginEtrace();
        sqlSessionContext
            .clientWriteAndFlush(Unpooled.wrappedBuffer(cmdHandshake.getSendBuf()), future -> {
                sqlSessionContext.sqlSessionContextUtil
                    .appendLoginPhrase(Constants.MYSQL_HANDSHAKE_SENT);
            });
        return false;
    }

}
