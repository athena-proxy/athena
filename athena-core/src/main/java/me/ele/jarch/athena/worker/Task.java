package me.ele.jarch.athena.worker;

import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.util.NoThrow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public abstract class Task implements Runnable, Comparable<Task> {

    private static final Logger logger = LoggerFactory.getLogger(Task.class);
    protected final SqlSessionContext ctx;
    private final String name;
    private final TaskPriority priority;
    private final long birthday;
    private static final AtomicLong sequenceGenerator = new AtomicLong(0);

    public TaskPriority getPriority() {
        return priority;
    }

    public String getName() {
        return name;
    }

    public Task(String name, SqlSessionContext ctx, TaskPriority priority) {
        this.name = name;
        this.ctx = Objects
            .requireNonNull(ctx, () -> "SqlSessionContext must be not null, msg: " + toString());
        this.priority = priority;
        birthday = sequenceGenerator.incrementAndGet();
    }

    public Task(String name, SqlSessionContext ctx) {
        this(name, ctx, TaskPriority.NORMAL);
    }

    public abstract void execute();

    @Override public void run() {
        NoThrow.call(() -> {
            try {
                if (logger.isDebugEnabled()) {
                    logger.debug(Objects.toString(this));
                }
                execute();
            } catch (Exception t) {
                logger.error(toString(), t);
                ctx.doQuit();
            }
        });
    }

    @Override public String toString() {
        return name + "|" + priority + "|" + birthday + "|" + ctx;
    }

    @Override public int compareTo(Task o) {
        int result = this.priority.getValue() - o.priority.getValue();
        return result == 0 ? Long.compare(this.birthday, o.birthday) : result;
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        Task task = (Task) o;

        if (birthday != task.birthday)
            return false;
        if (ctx != null ? !ctx.equals(task.ctx) : task.ctx != null)
            return false;
        if (name != null ? !name.equals(task.name) : task.name != null)
            return false;
        return priority == task.priority;

    }

    @Override public int hashCode() {
        int result = ctx != null ? ctx.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (priority != null ? priority.hashCode() : 0);
        result = 31 * result + (int) (birthday ^ (birthday >>> 32));
        return result;
    }

    public int getIndex() {
        return ctx.workerIdx;
    }
}
