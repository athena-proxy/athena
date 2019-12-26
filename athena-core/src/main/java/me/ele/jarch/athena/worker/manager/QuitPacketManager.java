package me.ele.jarch.athena.worker.manager;

import me.ele.jarch.athena.netty.SessionQuitTracer;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.worker.TaskPriority;

public class QuitPacketManager extends Manager {

    public QuitPacketManager(SqlSessionContext ctx) {
        super(QuitPacketManager.class.getName(), ctx, TaskPriority.HIGH);
    }

    @Override protected void manage() {
        ctx.quitTracer.reportQuit(SessionQuitTracer.QuitTrace.ClientNormalQuit);
        ctx.closeClientChannel();
    }
}
