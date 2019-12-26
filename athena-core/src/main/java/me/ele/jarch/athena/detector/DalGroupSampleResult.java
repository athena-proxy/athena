package me.ele.jarch.athena.detector;

/**
 * Created by Dal-Dev-Team on 17/3/3.
 */
public class DalGroupSampleResult extends AbstractSampleResult {
    // 瞬时队列数
    protected long curMasterQueueSize = 0;
    protected long curSlaveQueueSize = 0;

    protected long maxQueueSize = 0;

    public long getCurMasterQueueSize() {
        return curMasterQueueSize;
    }

    public void setCurMasterQueueSize(long curMasterQueueSize) {
        this.curMasterQueueSize = curMasterQueueSize;
        this.maxQueueSize = Math.max(this.maxQueueSize, this.curMasterQueueSize);
    }

    public long getCurSlaveQueueSize() {
        return curSlaveQueueSize;
    }

    public void setCurSlaveQueueSize(long curSlaveQueueSize) {
        this.curSlaveQueueSize = curSlaveQueueSize;
        this.maxQueueSize = Math.max(this.maxQueueSize, this.curSlaveQueueSize);
    }

    public long getMaxQueueSize() {
        return maxQueueSize;
    }
}
