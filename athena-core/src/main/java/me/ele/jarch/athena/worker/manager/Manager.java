package me.ele.jarch.athena.worker.manager;

import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.worker.Task;
import me.ele.jarch.athena.worker.TaskPriority;

public abstract class Manager extends Task {

    public Manager(String name, SqlSessionContext ctx) {
        super(name, ctx);
    }

    public Manager(String name, SqlSessionContext ctx, TaskPriority priority) {
        super(name, ctx, priority);
    }

    @Override public void execute() {
        manage();
    }

    protected abstract void manage();
}
