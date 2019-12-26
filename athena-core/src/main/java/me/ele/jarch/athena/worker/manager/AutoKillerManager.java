package me.ele.jarch.athena.worker.manager;

import com.github.mpjct.jmpjct.util.ErrorCode;
import me.ele.jarch.athena.netty.SessionQuitTracer;
import me.ele.jarch.athena.netty.SqlSessionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AutoKillerManager extends Manager {

    private static final Logger logger = LoggerFactory.getLogger(AutoKillerManager.class);


    public enum KillerStatus {
        KILL_SQL, KILL_TRANS
    }


    final KillerStatus status;

    public AutoKillerManager(SqlSessionContext ctx, KillerStatus status) {
        super(AutoKillerManager.class.getName() + "." + status.toString(), ctx);
        this.status = status;
    }

    private void doKillSql() {
        ctx.quitTracer.reportQuit(SessionQuitTracer.QuitTrace.AutoKillSlowSQL);
        ctx.kill(ErrorCode.ABORT_AUTOKILL_SLOWSQL, "auto kill slow SQL");
    }

    private void doKillTrans() {
        ctx.quitTracer.reportQuit(SessionQuitTracer.QuitTrace.AutoKillSlowTrans);
        ctx.kill(ErrorCode.ABORT_AUTOKILL_SLOWTRANS, "auto kill slow Transaction");
    }

    private void defaultPrint() {
        logger.error("Unknown status:" + status);
    }

    @Override protected void manage() {
        switch (status) {
            case KILL_SQL:
                doKillSql();
                break;
            case KILL_TRANS:
                doKillTrans();
                break;
            default:
                defaultPrint();

        }
    }

}
