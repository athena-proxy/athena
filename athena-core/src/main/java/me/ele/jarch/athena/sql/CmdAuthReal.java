package me.ele.jarch.athena.sql;

import com.github.mpjct.jmpjct.mysql.proto.Flags;
import com.github.mpjct.jmpjct.mysql.proto.Handshake;
import com.github.mpjct.jmpjct.mysql.proto.HandshakeResponse;
import me.ele.jarch.athena.allinone.DBConnectionInfo;
import me.ele.jarch.athena.exception.QuitException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CmdAuthReal extends OnePacketCommand {
    private static final Logger logger = LoggerFactory.getLogger(CmdAuthReal.class);

    private Handshake handshake;
    private DBConnectionInfo dbConnInfo;
    String schema;
    public String userName;

    public CmdAuthReal(DBConnectionInfo info, Handshake handshake) {
        super(new byte[0]);
        this.handshake = handshake;
        this.dbConnInfo = info;
    }

    @Override public boolean doInnerExecute() throws QuitException {
        HandshakeResponse authReply = new HandshakeResponse(dbConnInfo, handshake);
        if (!authReply.hasCapabilityFlag(Flags.CLIENT_PROTOCOL_41)) {
            throw new QuitException("We do not support Protocols under 4.1");
        }
        try {
            authReply.pack();
        } catch (Exception e) {
            logger.error("error in packing handshake response.", e);
            throw new QuitException("error in packing handshake response.");
        }
        schema = authReply.schema;
        userName = authReply.username;
        sendBuf = authReply.toPacket();
        return true;
    }
}
