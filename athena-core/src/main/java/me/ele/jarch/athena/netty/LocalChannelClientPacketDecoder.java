package me.ele.jarch.athena.netty;

import com.github.mpjct.jmpjct.mysql.proto.Com_Query;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.netty.state.SESSION_STATUS;
import me.ele.jarch.athena.scheduler.DBChannelDispatcher;
import me.ele.jarch.athena.sql.EleMetaParser;
import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

/**
 * Created by zhengchao on 16/8/29.
 */
public class LocalChannelClientPacketDecoder extends MySqlClientPacketDecoder {
    public LocalChannelClientPacketDecoder(boolean bind2Master, String remoteAddr) {
        super(bind2Master, remoteAddr);
        this.sqlCtx.authenticator.userName = "LocalClient";
        this.sqlCtx.authenticator.isAuthenticated = true;
        this.sqlCtx.setState(SESSION_STATUS.QUERY_ANALYZE);
    }

    protected void commentDecodeAndSet(byte[] packet) {
        Com_Query comQuery = Com_Query.loadFromPacket(packet);
        this.sqlCtx.batchCond
            .setBatchSendComment(EleMetaParser.parse(comQuery.getQuery()).getQueryComment());
        String dalGroupName =
            this.sqlCtx.batchCond.getValueFromComment(Constants.ELE_META_DAL_GROUP);
        boolean isFirstConnected = Objects.isNull(this.holder);
        boolean hasValidDalGroup = StringUtils.isNotEmpty(dalGroupName);
        if (hasValidDalGroup) {
            this.holder = DBChannelDispatcher.getHolders().get(dalGroupName);
            this.sqlCtx.setHolder(this.holder);
            this.sqlCtx.shardingContext = new ShardingContext(this.sqlCtx.quitTracer);
        }
        if (isFirstConnected && hasValidDalGroup) {
            this.holder.cCnt.incrementAndGet();
            this.holder.opencCnt.incrementAndGet();
        }
        String autoCommit =
            this.sqlCtx.batchCond.getValueFromComment(Constants.ELE_META_AUTO_COMMIT_FOR_CLIENT);
        if (Objects.nonNull(autoCommit)) {
            this.sqlCtx.transactionController.setAutoCommit(Boolean.valueOf(autoCommit));
        }
        String transId = this.sqlCtx.batchCond.getValueFromComment(Constants.ELE_META_TRANSID);
        if (StringUtils.isNotEmpty(transId)) {
            this.sqlCtx.transactionId = transId;
        }
        String bind2Master =
            this.sqlCtx.batchCond.getValueFromComment(Constants.ELE_META_BIND_MASTER);
        if (Objects.nonNull(bind2Master)) {
            this.sqlCtx.bind2master = Boolean.valueOf(bind2Master);
        } else {
            this.sqlCtx.bind2master = false;
        }
    }
}
