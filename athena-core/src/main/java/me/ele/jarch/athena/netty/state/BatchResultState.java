package me.ele.jarch.athena.netty.state;

import com.github.mpjct.jmpjct.mysql.proto.OK;
import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.netty.BatchSessionContext;
import me.ele.jarch.athena.server.async.AbstractAsyncResultHandler;
import me.ele.jarch.athena.sql.ResultSet;
import me.ele.jarch.athena.worker.SchedulerWorker;
import me.ele.jarch.athena.worker.manager.BatchDBResultManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Queue;

/**
 * Created by zhengchao on 16/8/29.
 */
public class BatchResultState extends AbstractAsyncResultHandler implements State {
    private static final Logger logger = LoggerFactory.getLogger(BatchResultState.class);
    private final BatchSessionContext batchContext;
    private MergeResultSet mergeRs = new MergeResultSet();
    private MergeOkResultSet mergeOk = new MergeOkResultSet();
    private ResultSet currentResultSet = null;

    public BatchResultState(BatchSessionContext batchContext) {
        this.batchContext = Objects.requireNonNull(batchContext,
            () -> "BatchContext must be not null, msg: " + toString());
    }

    @Override public boolean handle() throws QuitException {
        if (this.currentResultSet == null) {
            return false;
        }
        if (this.currentResultSet.hasOKPacket()) {
            mergeOk.handle(currentResultSet);
        } else {
            mergeRs.handle(currentResultSet);
        }
        currentResultSet = null;
        return false;
    }

    public void setResultSet(ResultSet rs) {
        this.currentResultSet = rs;
    }

    @Override public void handleResultSet(ResultSet resultSet, String localClientId) {
        if (resultSet == null) {
            logger.error("{},batchClientId={},resultSet is null", batchContext.batchTransLogIdStr,
                localClientId);
            return;
        }

        SchedulerWorker.getInstance()
            .enqueue(new BatchDBResultManager(batchContext, resultSet, localClientId));
    }

    @Override public SESSION_STATUS getStatus() {
        return SESSION_STATUS.BATCH_RESULT;
    }

    @Override public String toString() {
        return "BatchResultState";
    }

    public void reset() {
        mergeRs = new MergeResultSet();
        mergeOk = new MergeOkResultSet();
        currentResultSet = null;
    }

    private class MergeResultSet {
        protected ResultSet resultSet = null;

        public void handle(ResultSet resultSet) {
            if (resultSet == null) {
                return;
            }
            merge(resultSet);
            trySendBack();
        }

        protected void merge(ResultSet resultSet) {
            this.resultSet = resultSet;
        }

        protected void trySendBack() {
            if (batchContext.trySendBack2UserOrCloseUser(getPackets())) {
                reset();
            }
        }

        protected Queue<byte[]> getPackets() {
            return new ArrayDeque<>(resultSet.toPackets());
        }

        protected void reset() {
            resultSet = null;
        }

    }


    private class MergeOkResultSet extends MergeResultSet {
        protected OK okPacket = OK.loadFromPacket(OK.getOkPacketBytes());

        @Override protected void merge(ResultSet resultSet) {
            OK currentOk = resultSet.getOK();
            if (currentOk == null) {
                return;
            }
            this.okPacket.affectedRows += currentOk.affectedRows;
            this.okPacket.warnings += currentOk.warnings;
            this.okPacket.setStatusFlag(currentOk.statusFlags);
        }

        @Override protected Queue<byte[]> getPackets() {
            Queue<byte[]> packets = new ArrayDeque<>();
            packets.add(this.okPacket.toPacket());
            return packets;
        }

        @Override protected void reset() {
            logger.info("{} affected rows:{}", batchContext.batchTransLogIdStr,
                okPacket.affectedRows);
            super.reset();
            okPacket = OK.loadFromPacket(OK.getOkPacketBytes());
        }
    }
}
