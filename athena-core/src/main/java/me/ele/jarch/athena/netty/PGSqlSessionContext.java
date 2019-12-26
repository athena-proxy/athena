package me.ele.jarch.athena.netty;

import com.github.mpjct.jmpjct.util.Authenticator;
import com.github.mpjct.jmpjct.util.ErrorCode;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import me.ele.jarch.athena.netty.state.*;
import me.ele.jarch.athena.pg.proto.BackendKeyData;
import me.ele.jarch.athena.pg.proto.ErrorResponse;
import me.ele.jarch.athena.pg.proto.ReadyForQuery;
import me.ele.jarch.athena.pg.util.PGAuthenticator;
import me.ele.jarch.athena.pg.util.Severity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Queue;

public class PGSqlSessionContext extends SqlSessionContext {
    @SuppressWarnings("unused") private static final Logger LOGGER =
        LoggerFactory.getLogger(PGSqlSessionContext.class);

    private final BackendKeyData backendKeyData =
        new BackendKeyData((int) connectionId, BackendKeyData.genKey());

    public PGSqlSessionContext(SqlClientPacketDecoder sqlClientPacketDecoder, boolean bind2master,
        String remoteAddr) {
        super(sqlClientPacketDecoder, bind2master, remoteAddr);
        clientPackets = new ArrayDeque<>();
    }

    @Override protected TransactionController newTransactionController() {
        return new PGTransactionController();
    }

    @Override protected ClientFakeHandshakeState newClientFakeHandshakeState() {
        return new PGClientFakeHandshakeState(this);
    }

    @Override protected ClientFakeAuthState newClientFakeAuthState() {
        return new PGClientFakeAuthState(this);
    }

    @Override protected QueryAnalyzeState newQueryAnalyzeState() {
        return new PGQueryAnalyzeState(this);
    }

    @Override protected QueryHandleState newQueryHandleState() {
        return new PGQueryHandleState(this);
    }

    @Override protected QueryResultState newQueryResultState() {
        return new PGQueryResultState(this);
    }

    @Override protected FakeSqlState newFakeSqlState() {
        return new PGFakeSqlState(this);
    }

    @Override protected Authenticator newAuthenticator() {
        return new PGAuthenticator();
    }

    @Override public ShardingContext newShardingContext(SessionQuitTracer quitTracer) {
        return new PGShardingContext(quitTracer);
    }

    @Override public void bindClientPackets(Queue<byte[]> clientPackets) {
        this.clientPackets.addAll(clientPackets);
    }

    @Override protected ByteBuf buildKillErr(long errorNo, String sqlState, String dalMessage,
        long sequenceId) {
        ByteBuf buf = Unpooled.buffer();
        ErrorResponse errorResponse = ErrorResponse
            .buildErrorResponse(Severity.FATAL, sqlState, dalMessage, getClass().getSimpleName(),
                "1", "dal_kill");
        buf.writeBytes(errorResponse.toPacket());
        buf.writeBytes(isInTransStatus() ?
            ReadyForQuery.IN_FAILED_TX.toPacket() :
            ReadyForQuery.IDLE.toPacket());
        return buf;
    }

    public BackendKeyData getBackendKeyData() {
        return backendKeyData;
    }

    @Override public boolean allowUnsyncQuery() {
        return true;
    }

    @Override public ByteBuf buildErr(ErrorCode errorCode, String errMsg, String routine) {
        return buildEzoneErr(errMsg, routine);
    }

    private ByteBuf buildEzoneErr(String message, String routine) {
        ByteBuf buf = Unpooled.buffer();
        ErrorResponse errorResponse = ErrorResponse
            .buildErrorResponse(Severity.FATAL, ErrorCode.DAL_REBALANCE.getSqlState(), message,
                getClass().getSimpleName(), "1", routine);
        buf.writeBytes(errorResponse.toPacket());
        buf.writeBytes(isInTransStatus() ?
            ReadyForQuery.IN_FAILED_TX.toPacket() :
            ReadyForQuery.IDLE.toPacket());
        return buf;
    }

    @Override public void setSwallowQueryResponse(boolean swallowQueryResponse) {
        this.swallowQueryResponse = swallowQueryResponse;
    }
}
