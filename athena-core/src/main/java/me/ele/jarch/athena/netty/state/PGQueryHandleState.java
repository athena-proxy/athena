package me.ele.jarch.athena.netty.state;

import com.google.common.primitives.Bytes;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.pg.proto.Query;
import me.ele.jarch.athena.server.pool.ServerSession;
import me.ele.jarch.athena.sql.CmdQuery;

/**
 * Created by jinghao.wang on 16/12/2.
 */
public class PGQueryHandleState extends QueryHandleState {
    public PGQueryHandleState(SqlSessionContext sqlSessionContext) {
        super(sqlSessionContext);
    }

    @Override protected boolean isMysqlComFieldList(byte type) {
        return false;
    }

    @Override protected byte[] assembleFirstSendBuf(CmdQuery query) {
        if (!shouldPrependBegin()) {
            return query.getSendBuf();
        }
        sqlSessionContext.setSwallowQueryResponse(true);
        return Bytes.concat(Query.getBeginBytes(), query.getSendBuf());
    }

    private boolean shouldPrependBegin() {
        return sqlSessionContext.transactionController.canOpenTransaction() && sqlSessionContext
            .isInTransStatus();
    }

    @Override protected void startDBEtraceTransaction(ServerSession session) {
        super.startDBEtraceTransaction(session);
        if (sqlSessionContext.isSwallowQueryResponse()) {
            sqlSessionContext.sqlSessionContextUtil
                .startNewTransaction(session, "unkowntable.begin", Query.getBeginBytes().length);
        }
    }
}
