package me.ele.jarch.athena.server.async;

import com.github.mpjct.jmpjct.mysql.proto.ERR;

public abstract class AbstractAsyncErrorHandler implements AsyncErrorHandler {

    @Override public void handleError(ERR_TYPE reason, String params) {
        ERR err = new ERR();
        handleError(reason, err, params);
    }

    public abstract void handleError(ERR_TYPE reason, ERR errPacket, String params);
}
