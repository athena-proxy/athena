package me.ele.jarch.athena.sql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CmdQueryResult extends ComposableCommand {
    public static final Logger logger = LoggerFactory.getLogger(CmdQueryResult.class);

    QueryResultContext ctx;

    public CmdQueryResult(QueryResultContext ctx) {
        this.ctx = ctx;
        init();
    }

    protected void init() {
        appendCmd(new CmdColCnt(ctx, this));
    }

    @Override public boolean doInnerExecute() {
        return true;
    }

    public QueryResultContext getCtx() {
        return ctx;
    }
}
