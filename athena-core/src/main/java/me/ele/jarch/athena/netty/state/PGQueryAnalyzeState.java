package me.ele.jarch.athena.netty.state;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.exception.QueryException;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.pg.proto.*;
import me.ele.jarch.athena.pg.util.Severity;
import me.ele.jarch.athena.sql.CmdQuery;
import me.ele.jarch.athena.sql.PGCmdQuery;
import me.ele.jarch.athena.sql.QUERY_TYPE;
import me.ele.jarch.athena.sql.QueryPacket;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

/**
 * Created by jinghao.wang on 16/11/28.
 */
public class PGQueryAnalyzeState extends QueryAnalyzeState {
    public PGQueryAnalyzeState(SqlSessionContext sqlSessionContext) {
        super(sqlSessionContext);
    }

    @Override protected CmdQuery newCmdQuery(byte[] packet, String dalGroup) {
        return new PGCmdQuery(packet, dalGroup);
    }

    @Override protected QueryPacket newQueryPacket() {
        return new Query();
    }

    /**
     * 禁用针对mysql jdbc PrepareStmt的拒绝逻辑
     *
     * @param curCmdQuery
     * @return always return true
     * @throws QueryException
     */
    @Override protected boolean validateJDBCPrepareStmt(CmdQuery curCmdQuery)
        throws QueryException {
        return true;
    }

    protected Optional<ByteBuf> tryFakeEarlyReturnQueryResponse(CmdQuery curCmdQuery,
        boolean fakeCommit_Rollback) {
        ByteBuf fakeResponse = null;
        // 处理需要伪造具体ResultSet结果集的SQL
        if (curCmdQuery.queryType.isFakeResultStatement()) {
            switch (curCmdQuery.queryType) {
                case PG_EMPTY_QUERY:
                    fakeResponse = handlePGEmptyQuery();
                    break;
                default:
                    throw new IllegalStateException(
                        "unexpected fake result query type: " + curCmdQuery.queryType);
            }
            // 处理pg begin sql,除了返回伪造结果以外,还要进行一些设置
        } else if (curCmdQuery.queryType == QUERY_TYPE.BEGIN) {
            fakeResponse = handleBegin();
            // 处理普通SET
        } else if (curCmdQuery.queryType == QUERY_TYPE.IGNORE_CMD) {
            fakeResponse = handleIgnoreCmd();
        } else if (fakeCommit_Rollback) {
            // 处理不在事务中得commit和rollback
            fakeResponse = handleFakeCommitRollback(curCmdQuery);
        }
        return Optional.ofNullable(fakeResponse);
    }

    /**
     * {@inheritDoc}
     * 由于是PG特有协议,所以应该调用此处方法
     *
     * @return 伪造的空查询结果集
     */
    private ByteBuf handlePGEmptyQuery() {
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes(EmptyQueryResponse.INSTANCE.toPacket());
        buf.writeBytes(buildRightReadyForQueryPacket());
        return buf;
    }

    @Override protected ByteBuf handleBegin() {
        ByteBuf buf = Unpooled.buffer();
        if (sqlSessionContext.transactionController.hasImplicitTransactionMarker()) {
            List<NoticeResponse.Notice> fields = new LinkedList<>();
            fields.add(new NoticeResponse.Notice(PGFlags.SEVERITY, Severity.WARNING.name()));
            fields.add(new NoticeResponse.Notice(PGFlags.CODE, "25001"));
            fields.add(new NoticeResponse.Notice(PGFlags.MESSAGE,
                Constants.DAL + " there is already a transaction in progress"));
            fields.add(new NoticeResponse.Notice(PGFlags.FILE, getClass().getSimpleName()));
            fields.add(new NoticeResponse.Notice(PGFlags.LINE, "82"));
            fields.add(new NoticeResponse.Notice(PGFlags.ROUTINE, "BeginTransactionBlock"));
            NoticeResponse noticeResponse = new NoticeResponse(fields);
            buf.writeBytes(noticeResponse.toPacket());
        } else {
            //PG `BEGIN` 的作用就是开启隐式的事务块
            sqlSessionContext.transactionController.markImplicitTransaction();
        }
        buf.writeBytes(new CommandComplete("BEGIN").toPacket());
        buf.writeBytes(ReadyForQuery.IN_TX.toPacket());
        return buf;
    }

    @Override protected ByteBuf handleFakeCommitRollback(CmdQuery curCmdQuery) {
        ByteBuf buf = Unpooled.buffer();
        if (!sqlSessionContext.transactionController.hasImplicitTransactionMarker()) {
            NoticeResponse notices = new NoticeResponse();
            notices.addNotice(PGFlags.SEVERITY, "WARNING");
            notices.addNotice(PGFlags.CODE, "25P01");
            notices
                .addNotice(PGFlags.MESSAGE, Constants.DAL + "there is no transaction in progress");
            notices.addNotice(PGFlags.FILE, "dal_pg.c");
            notices.addNotice(PGFlags.LINE, "1");
            notices.addNotice(PGFlags.ROUTINE, "dal_default_routine");
            // notice reponse
            buf.writeBytes(notices.toPacket());
        } else {
            // 处理BEGIN -> SELECT -> COMMIT场景
            sqlSessionContext.transactionController.onTransactionEnd();
        }
        // command complete
        CommandComplete cc = new CommandComplete(
            curCmdQuery.curQueryType == QUERY_TYPE.COMMIT ? "COMMIT" : "ROLLBACK");
        buf.writeBytes(cc.toPacket());
        buf.writeBytes(ReadyForQuery.IDLE.toPacket());
        return buf;
    }

    @Override protected ByteBuf handleIgnoreCmd() {
        CommandComplete cc = new CommandComplete("SET");
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes(cc.toPacket());
        buf.writeBytes(buildRightReadyForQueryPacket());
        return buf;
    }

    /**
     * 禁止针对mysql COM_FIELD_LIST 命令不记录`Othen SQL type`的操作
     *
     * @param type
     * @return
     */
    @Override protected boolean isLogSQLType(byte type) {
        return true;
    }

    private byte[] buildRightReadyForQueryPacket() {
        return sqlSessionContext.isInTransStatus() ?
            ReadyForQuery.IN_TX.toPacket() :
            ReadyForQuery.IDLE.toPacket();
    }

    /**
     * PostgreSQL do not support index hints
     *
     * @param cmdQuery
     */
    @Override protected void rewriteIndexHintsIfConfigured(CmdQuery cmdQuery) {
        // do nothing
    }
}
