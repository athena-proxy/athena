package me.ele.jarch.athena.netty.state;

import me.ele.jarch.athena.exception.QuitException;

public interface State {
    boolean handle() throws QuitException;

    SESSION_STATUS getStatus();
}
