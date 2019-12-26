package me.ele.jarch.athena.worker.manager;

import me.ele.jarch.athena.netty.SqlSessionContext;

public class ClientLoginTimeoutManager extends Manager {

    private Runnable task;

    public ClientLoginTimeoutManager(SqlSessionContext ctx, Runnable task) {
        super(ClientLoginTimeoutManager.class.getName(), ctx);
        this.task = task;
    }

    @Override protected void manage() {
        task.run();
    }
}
