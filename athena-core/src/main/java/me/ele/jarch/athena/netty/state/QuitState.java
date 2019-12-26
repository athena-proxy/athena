package me.ele.jarch.athena.netty.state;

import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.netty.SqlSessionContext;

public class QuitState implements State {
    private SqlSessionContext sqlSessionContext;

    public QuitState(SqlSessionContext sqlSessionContext) {
        this.sqlSessionContext = sqlSessionContext;
    }

    @Override public boolean handle() throws QuitException {
        return sqlSessionContext.doQuit();
    }

    @Override public SESSION_STATUS getStatus() {
        return SESSION_STATUS.QUIT;
    }

}
