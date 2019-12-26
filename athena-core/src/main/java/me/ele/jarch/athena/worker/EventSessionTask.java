package me.ele.jarch.athena.worker;

import me.ele.jarch.athena.netty.SqlSessionContext;

import java.util.Queue;

public class EventSessionTask extends SessionTask {

    private final Queue<byte[]> packets;

    public EventSessionTask(SqlSessionContext ctx, Queue<byte[]> packets) {
        super(EventSessionTask.class.getName(), ctx, TaskPriority.NORMAL);
        this.packets = packets;
    }

    @Override public void execute() {
        ctx.setDbServerPackets(this.packets);
        super.execute();
    }

}
