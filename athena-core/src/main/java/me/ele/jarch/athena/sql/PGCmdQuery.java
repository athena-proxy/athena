package me.ele.jarch.athena.sql;

import com.github.mpjct.jmpjct.util.ErrorCode;
import me.ele.jarch.athena.exception.QueryException;
import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.pg.proto.*;
import me.ele.jarch.athena.sharding.ShardingRouter;
import me.ele.jarch.athena.sharding.sql.Send2BatchCond;
import me.ele.jarch.athena.sharding.sql.ShardingSQL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.function.Predicate;

/**
 * Created by jinghao.wang on 16/11/28.
 */
public class PGCmdQuery extends CmdQuery {
    @SuppressWarnings("unused") private static final Logger LOGGER =
        LoggerFactory.getLogger(PGCmdQuery.class);

    public PGCmdQuery(byte[] packet, String dalGroup) {
        super(packet, dalGroup);
    }

    private void checkExtendedQuery() {
        if (type == PGFlags.C_QUERY || type == PGFlags.C_SYNC) {
            return;
        }
        PGMessage message = null;
        switch (type) {
            case PGFlags.C_PARSE:
                message = Parse.loadFromPacket(packet);
                break;
            case PGFlags.C_BIND:
                message = Bind.loadFromPacket(packet);
                break;
            case PGFlags.C_EXECUTE:
                message = Execute.loadFromPacket(packet);
                break;
        }
        // 给query赋值,表示客户端处于接收Query的状态
        // 此时抛出异常后,DAL会回复ErrorResponse给客户端 
        if (Objects.nonNull(message)) {
            this.query = message.toString();
        }
        String errMsg = Objects.nonNull(message) ?
            "Extended Query is not supported: " + message :
            "Unknown Query Type: " + (byte) type;
        throw new QueryException.Builder(ErrorCode.ER_SYNTAX_ERROR).setSequenceId(1)
            .setErrorMessage(errMsg).bulid();
    }

    @Override protected boolean doInnerExecute() throws QuitException {
        type = PGMessage.getType(packet);
        typeName = PGMessage.getClientPacketTypeName(type);
        checkExtendedQuery();

        Query comQuery = null;
        if (type == PGFlags.C_SYNC) {
            // 当类型为SYNC时,有特殊逻辑
            // 和MYSQL_PING类似,模拟一条SQL,用于日志打印
            // 但真实的数据传输仍然使用原先的packet
            comQuery = new Query(SYNC_SQL);
        } else {
            comQuery = Query.loadFromPacket(packet);
            if (isEmptyQuery(comQuery.getQuery())) {
                //处理pg empty query 逻辑
                comQuery.setQuery(EMPTY_SQL);
                handleQuery(comQuery);
                queryType = QUERY_TYPE.PG_EMPTY_QUERY;
                return false;
            }
        }
        handleQuery(comQuery);
        if (type == PGFlags.C_SYNC) {
            queryType = QUERY_TYPE.SYNC;
            sendBuf = packet;
        }
        return true;
    }

    @Override protected QueryPacket newQueryPacket() {
        return new Query();
    }

    private boolean isEmptyQuery(String query) {
        return query.isEmpty();
    }

    @Override
    protected ShardingSQL handleSQLBasicly(String queryWithoutComment, String queryComment,
        ShardingRouter shardingRouter, Send2BatchCond batchCond,
        Predicate<String> whiteFieldsFilter) {
        return ShardingSQL
            .handlePGBasicly(queryWithoutComment, queryComment, shardingRouter, batchCond,
                whiteFieldsFilter);
    }
}
