package me.ele.jarch.athena.sql;

import me.ele.jarch.athena.exception.QuitException;

import java.util.ArrayList;
import java.util.List;

public abstract class ComposableCommand {

    private volatile boolean isDone = false;

    final public boolean isDone() {
        return isDone;
    }

    protected abstract boolean doInnerExecute() throws QuitException;

    private List<ComposableCommand> subCmds = new ArrayList<>();

    final public void appendCmd(ComposableCommand cmd) {
        subCmds.add(cmd);
    }

    // store the index to optimize
    private volatile int index = 0;

    public void execute() throws QuitException {
        if (this.isDone())
            return;
        for (; index < subCmds.size(); ++index) {
            ComposableCommand cmd = subCmds.get(index);
            cmd.execute();
            if (!cmd.isDone())
                return;
            else
                // set done task to DONE_CMD for early release old commands
                subCmds.set(index, DONE_CMD);
        }
        isDone = doInnerExecute();
    }

    private static final ComposableCommand DONE_CMD = new ComposableCommand() {

        @Override protected boolean doInnerExecute() {
            return true;
        }
    };
}
