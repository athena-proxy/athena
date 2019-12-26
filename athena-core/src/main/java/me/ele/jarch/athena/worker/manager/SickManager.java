package me.ele.jarch.athena.worker.manager;

import com.github.mpjct.jmpjct.util.ErrorCode;
import me.ele.jarch.athena.netty.SessionQuitTracer;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.util.ResponseStatus;

public class SickManager extends Manager {
    private static final String sickMessageTemplate = "sql sick '%s'";
    private final String sickMessage;

    public SickManager(SqlSessionContext ctx, String qualifiedDbId) {
        super(SickManager.class.getName(), ctx);
        sickMessage = String.format(sickMessageTemplate, qualifiedDbId);
    }

    private void doKill() {
        ctx.quitTracer.reportQuit(SessionQuitTracer.QuitTrace.DBFuse,
            new ResponseStatus(ResponseStatus.ResponseType.DALERR, ErrorCode.DALSICK, sickMessage));
        ctx.kill(ErrorCode.DALSICK, sickMessage);
    }

    @Override protected void manage() {
        doKill();
    }

}
