package me.ele.jarch.athena.netty;

import com.github.mpjct.jmpjct.mysql.proto.Flags;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.sql.CmdTcpPacket;
import me.ele.jarch.athena.sql.MySQLCmdTcpPacket;
import me.ele.jarch.athena.worker.SchedulerWorker;
import me.ele.jarch.athena.worker.manager.HandshakeManager;

import static me.ele.jarch.athena.netty.state.SESSION_STATUS.CLIENT_FAKE_AUTH;

public class MySqlClientPacketDecoder extends SqlClientPacketDecoder {

    public MySqlClientPacketDecoder(boolean bind2Master, String remoteAddr) {
        super(bind2Master, remoteAddr);
    }

    @Override
    protected SqlSessionContext newSqlSessionContext(boolean bind2Master, String remoteAddr) {
        return new SqlSessionContext(this, bind2Master, remoteAddr);
    }

    @Override protected void handshakeOnActive() {
        SchedulerWorker.getInstance().enqueue(new HandshakeManager(sqlCtx));
    }

    @Override protected CmdTcpPacket newPacket() {
        return new MySQLCmdTcpPacket(inputbuf);
    }

    @Override protected boolean isQuitPacket(byte[] src) {
        if (src == null) {
            return false;
        }
        return src[0] == 1 && src[1] == 0 && src[2] == 0 && src[4] == Flags.COM_QUIT;
    }

    @Override protected void traceOnLoginPacketReceive(byte[] packet) {
        if (CLIENT_FAKE_AUTH != sqlCtx.getStatus()) {
            return;
        }
        sqlCtx.sqlSessionContextUtil.appendLoginPhrase(Constants.MYSQL_HANDSHAKE_RESPONSE_RECEIVED);
    }
}
