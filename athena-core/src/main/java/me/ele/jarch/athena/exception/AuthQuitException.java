package me.ele.jarch.athena.exception;

import io.netty.buffer.ByteBuf;

public class AuthQuitException extends QuitException {
    private static final long serialVersionUID = -5832264467442371374L;
    private ByteBuf authBuf = null;

    private AuthQuitState state = AuthQuitState.OTHER;

    public AuthQuitException(String message) {
        super(message);
    }

    public AuthQuitException(String message, AuthQuitState state) {
        super(message);
        this.state = state;
    }

    public ByteBuf getAuthBuf() {
        return authBuf;
    }

    public AuthQuitState getState() {
        return state;
    }

    public void setState(AuthQuitState state) {
        this.state = state;
    }

}
