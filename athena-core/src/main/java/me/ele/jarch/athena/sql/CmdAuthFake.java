package me.ele.jarch.athena.sql;

import com.github.mpjct.jmpjct.mysql.proto.Flags;
import com.github.mpjct.jmpjct.mysql.proto.HandshakeResponse;
import me.ele.jarch.athena.exception.QuitException;

public class CmdAuthFake extends OnePacketCommand {
    public String schema;
    public String userName;
    public byte[] passwordAfterEncryption;
    public String clientAttributes;

    public CmdAuthFake(byte[] packet) {
        super(packet);
    }

    @Override public boolean doInnerExecute() throws QuitException {
        // no need sendBuf
        HandshakeResponse authReply = HandshakeResponse.loadFromPacket(packet);
        if (!authReply.hasCapabilityFlag(Flags.CLIENT_PROTOCOL_41)) {
            throw new QuitException("We do not support Protocols under 4.1");
        }
        schema = authReply.schema;
        userName = authReply.username;
        passwordAfterEncryption = authReply.authResponse;
        clientAttributes = authReply.clientAttributes;
        return true;
    }
}
