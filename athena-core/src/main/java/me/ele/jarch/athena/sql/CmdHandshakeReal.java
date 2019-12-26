package me.ele.jarch.athena.sql;

import com.github.mpjct.jmpjct.mysql.proto.ResultSet;
import com.github.mpjct.jmpjct.mysql.proto.*;
import me.ele.jarch.athena.exception.AuthQuitException;

public class CmdHandshakeReal extends OnePacketCommand {

    public Handshake handshake;

    public CmdHandshakeReal(byte[] packet) {
        super(packet);
    }

    @Override public boolean doInnerExecute() throws AuthQuitException {
        if (Packet.getType(this.packet) == Flags.ERR) {
            throw new AuthQuitException(ERR.loadFromPacket(this.packet).errorMessage);
        }

        handshake = Handshake.loadFromPacket(packet);
        // Remove some flags from the reply
        handshake.removeCapabilityFlag(Flags.CLIENT_COMPRESS);
        handshake.removeCapabilityFlag(Flags.CLIENT_SSL);
        handshake.removeCapabilityFlag(Flags.CLIENT_LOCAL_FILES);

        // Set the default result set creation to the server's character set
        ResultSet.characterSet = handshake.characterSet;

        // convert to ByteBuf for Netty
        return true;
    }
}
