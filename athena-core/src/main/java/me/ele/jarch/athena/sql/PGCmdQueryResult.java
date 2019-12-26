package me.ele.jarch.athena.sql;

/**
 * Created by jinghao.wang on 16/11/28.
 */
public class PGCmdQueryResult extends CmdQueryResult {
    public PGCmdQueryResult(QueryResultContext ctx) {
        super(ctx);
    }

    @Override protected void init() {
        appendCmd(new PGCmdResponse(ctx, this));
    }
}
