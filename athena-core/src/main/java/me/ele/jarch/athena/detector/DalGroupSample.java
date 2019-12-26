package me.ele.jarch.athena.detector;

import me.ele.jarch.athena.allinone.DBRole;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.scheduler.DBChannelDispatcher;
import me.ele.jarch.athena.scheduler.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Dal-Dev-Team on 17/3/3.
 */
public class DalGroupSample extends AbstractSample {
    private static final Logger LOGGER = LoggerFactory.getLogger(DalGroupSample.class);

    public final DalGroupSampleResult result = new DalGroupSampleResult();

    @Override protected void calc() {
        if (curWindowDur == 0)
            return;
        result.setQps(1000 * count / (curWindowDur + 0.001d));
        //        result.setUpper90Dur(calcUpper90());
        result.setAvgDurInCurWindow(DetectUtil.div(durSumInCurWindow, count));
    }

    void calcQueueSize() {
        if (count == 0) {
            return;
        }
        DBChannelDispatcher dispatcher = DBChannelDispatcher.getHolders().get(dalGroup);
        for (Scheduler scheduler : dispatcher.getScheds()) {
            if (scheduler.getInfo().getRole() == DBRole.MASTER && scheduler.getInfo().isActive()) {
                int count = scheduler.getQueueBlockingTaskCount();
                result.setCurMasterQueueSize(Math.max(count, result.getCurMasterQueueSize()));
            } else if (scheduler.getInfo().getRole() == DBRole.SLAVE && scheduler.getInfo()
                .isActive()) {
                int count = scheduler.getQueueBlockingTaskCount();
                result.setCurSlaveQueueSize(Math.max(count, result.getCurSlaveQueueSize()));
            }
        }
    }

    @Override public void addSample(long dur, SqlSessionContext ctx) {
        this.dalGroup = ctx.getHolder().getDalGroup().getName();
        this.durSumInCurWindow += dur;
        this.count++;
        //        putDur(dur);
    }
}
