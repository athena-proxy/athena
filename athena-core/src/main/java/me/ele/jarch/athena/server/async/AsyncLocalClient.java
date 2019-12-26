package me.ele.jarch.athena.server.async;

import com.github.mpjct.jmpjct.mysql.proto.ERR;
import com.github.mpjct.jmpjct.util.ErrorCode;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import me.ele.jarch.athena.allinone.DBConnectionInfo;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.netty.AthenaEventLoopGroupCenter;
import me.ele.jarch.athena.server.pool.ServerSession;
import me.ele.jarch.athena.sql.BatchQuery;
import me.ele.jarch.athena.sql.ResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by zhengchao on 16/8/26.
 */
public class AsyncLocalClient extends AsyncClient {
    private static Logger logger = LoggerFactory.getLogger(AsyncLocalClient.class);
    private static final AtomicLong ADDR_ID_GENERATOR = new AtomicLong(0);
    /*@formatter:off
     * This `alive` is used to indicate that whether this client should be closed or not.
     * When it is false, the client should not be reused to execute further query again.
     *
     * The scenarios of using this flag are:
     * 1. When big ResultSet occured, in order to avoid processing later-on data packets,
     *    we choose to close the client and send back error packet, indicating big ResultSet
     *    occurrence. This is the time `alive` is set to false; when the later-on query finds
     *    this client is no longer alive, the batch state machine will doQuit this client
     *    and create a new one for its query.
     * 2. When the client's connection to db is inactive, `handleChannelInactive` is our
     *    processing logic.
     * @formatter:on
     * */
    private final AtomicBoolean alive = new AtomicBoolean(true);
    private final String id;
    private final LocalAddress localAddress;
    private final LocalAddress remoteAddress;
    private BatchQuery batchQuery;
    private AbstractAsyncResultHandler resultHandler;
    private AbstractAsyncErrorHandler errorHandler;
    private boolean callErrorHandler = true;
    private int accumulatedResultSize = 0;
    private String asyncClientTransLogIdStr;

    public AsyncLocalClient(String id) {
        super(new DBConnectionInfo());
        this.id = id;
        this.localAddress = new LocalAddress(
            String.format("batchclientid_%s_%d", this.id, ADDR_ID_GENERATOR.getAndIncrement()));
        this.remoteAddress = new LocalAddress(Constants.LOCAL_CHANNEL_SERVER_ADDRESS);
    }

    public void setBatchQuery(BatchQuery batchQuery) {
        this.batchQuery =
            Objects.requireNonNull(batchQuery, () -> "BatchQuery must not be null:" + toString());
    }

    public BatchQuery getBatchQuery() {
        return batchQuery;
    }

    public void setBatchTransLogIdStr(String batchTransLogIdStr) {
        asyncClientTransLogIdStr = String.format("%s,batchClientId=%s",
            Objects.isNull(batchTransLogIdStr) ? "" : batchTransLogIdStr, id);
    }

    public String getAsyncClientTransLogIdStr() {
        return asyncClientTransLogIdStr;
    }

    @Override protected String getQuery() {
        assert false : "fatal error, this should not be called";
        return this.batchQuery.getQuery();
    }

    public AsyncClient setHandler(AbstractAsyncResultHandler resultSetHandler,
        AbstractAsyncErrorHandler errorHandler) {
        this.resultHandler = resultSetHandler;
        this.errorHandler = errorHandler;
        if (this.resultHandler != null) {
            this.resultHandler.setErrorHandler(this.errorHandler);
        }
        return this;
    }

    @Override protected void doInit() throws QuitException {
        setStatus(ServerSession.SERVER_SESSION_STATUS.QUERY);
        Bootstrap b = new Bootstrap();
        b.localAddress(localAddress);
        b.group(AthenaEventLoopGroupCenter.getLocalWorkerGroup());
        b.channel(LocalChannel.class);
        b.handler(new ChannelInitializer<LocalChannel>() {
            @Override protected void initChannel(LocalChannel ch) throws Exception {
                AsyncLocalClient.this.sqlServerPackedDecoder
                    .setServerSession(AsyncLocalClient.this);
                ch.pipeline().addLast(AsyncLocalClient.this.sqlServerPackedDecoder);
            }
        });

        if (logger.isDebugEnabled()) {
            logger.debug(String.format("connecting to local server : %s", this.remoteAddress));
        }

        b.connect(remoteAddress).addListener(new ChannelFutureListener() {

            @Override public void operationComplete(ChannelFuture future) throws Exception {
                if (!future.isSuccess()) {
                    /* @formatter:off
                     * The choice we make here is to retry when we have encountered errors when local clients' connecting to local channel server.
                     * The immediate reason is netty(4.0.26.final) that we use at the very beginning. It generates channels
                     * which do not have unique global ids. Thus, it will make the latter connection to local channel server fail,
                     * an io.netty.channel.ChannelException with message of "address already in use by ....".
                     * This bug can be avoided if the netty can be upgraded to version 5.0.0 or later.
                     * This issue (https://github.com/netty/netty/pull/1983) can be followed.
                     *
                     * At this moment, we choose to retry infinitely as we donot want to expose athena's inner error to the end user.
                     * @formatter:on
                     * */
                    String errorMessage = String
                        .format("%s,local server connection failed,server: [%s], cause:[%s]",
                            asyncClientTransLogIdStr, AsyncLocalClient.this.remoteAddress,
                            future.cause() != null ? future.cause().toString() : "unknown");
                    logger.error(errorMessage);
                    doQuit("", false);
                    handleConnectFail();
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug(String.format("connected to local server: %s finished",
                            AsyncLocalClient.this.remoteAddress));
                    }
                    AsyncLocalClient.this.serverChannel = future.channel();
                    execute();
                }
            }
        });
    }

    @Override protected void doQuery() {
        setStatus(ServerSession.SERVER_SESSION_STATUS.QUERY_RESULT);
        dbServerWriteAndFlush(batchQuery.toPacket());
    }

    protected void doResult() throws QuitException {
        if (isResultSetBigEnough()) {
            handleBigResultSet();
        }

        super.doResult();
    }

    private boolean isResultSetBigEnough() {
        for (byte[] packet : serverPackets) {
            accumulatedResultSize += packet.length;
        }

        return accumulatedResultSize > Constants.SEND_CLIENT_MAX_BUFFER;
    }

    private void handleBigResultSet() {
        String errMsg = "At most 64K result set is allowed in batch transaction";
        doQuit(errMsg, false);
        logger.error(String.format("%s, query:%s, address:%s", errMsg, batchQuery.getQuery(),
            this.serverChannel.localAddress().toString()));
        this.result = null;

        ERR errorPacket = new ERR();
        errorPacket.sequenceId = 1;
        errorPacket.errorCode = ErrorCode.ERR_OVER_MAX_RESULT_BUF_SIZE.getErrorNo();
        errorPacket.errorMessage = errMsg;

        serverPackets.clear();
        serverPackets.add(errorPacket.toPacket());
    }

    /*@formatter:off
     Below is the sequence when the AsyncClient doQuit its connection to MySql, which illustrate the reason
     why we need to control the error handling in `handleChannelInactive`.
     +------------------------+
     |                        |                                     +---------------------------+
     |     AsyncLocalClient   |                                     |                           |
     |                        |                                     |   LocalChannelServer      |
     +------------------------+                                     |                           |
                |                                                   +---------------------------+
                |                                                           |
                |                                                           |
              +---+                                                         |
              |   | doQuit                                                  |
              |   |                                                         |
              |   +----+                                                    |
              +---+    |                                                    |
                |      |                                                    |
                |      |                                                    |
                |      |                                                    |
              +---+    |                                                    |
              |   <----+                                                    |
              |   | closeServerChannelGracefully                            |
              |   |                                                         |
              |   |                                                         |
              |   +-----------------------------------------------------> +----+
              +---+                send one QUIT packet                   |    |
                |                                                         |    |
                |                                                         |    |
                |                                                         |    |
                |                                                         +----+  closeConnection
              +---+
              |   |
              |   |
              |   | handleChannelInactive after
              |   | MySQL closes connection
              +---+

       When the big result set is found, `doQuit` is used to close the connection to mysql to avoid receiving
       further packet. `handleErrorWhenClosed` is used to do not handle error when we are doing this.
    @formatter:on
    */

    /**
     * if the client close quietly without influence the caller
     *
     * @param reason
     * @param handleErrorWhenClosed
     */
    public synchronized void doQuit(String reason, boolean handleErrorWhenClosed) {
        if (!isAlive()) {
            return;
        }
        this.callErrorHandler = handleErrorWhenClosed;
        super.doQuit(reason);
    }

    @Override
    protected void handleResultSet(ResultSet asyncResultSet) {
        resultHandler.handleResultSet(asyncResultSet, true, id);
    }

    @Override
    protected void handleTimeout() {
        assert false : "AsyncLocalClient.handleTimeout should not be called";
    }

    @Override
    protected void handleChannelInactive() {
        if (this.alive.getAndSet(false) && this.callErrorHandler) {
            logger.warn("{}, channel inactive, query:[{}]", toString(), batchQuery.getQuery());
            errorHandler.handleError(AsyncErrorHandler.ERR_TYPE.CHANNEL_INACTIVE, id);
        }
    }

    private void handleConnectFail() {
        this.alive.set(false);
        errorHandler.handleError(AsyncErrorHandler.ERR_TYPE.CONNECT_FAILED, id);
    }

    public boolean isAlive() {
        return this.alive.get();
    }

    @Override
    public String toString() {
        String localClientAddress = Objects.nonNull(serverChannel) ? serverChannel.localAddress().toString() : "unknown";
        return String.format("AsyncLocalClient, client address:[%s]", localClientAddress);
    }

    public String getId() {
        return id;
    }
}
