package me.ele.jarch.athena.sql;

import com.github.mpjct.jmpjct.mysql.proto.ERR;
import com.github.mpjct.jmpjct.mysql.proto.Flags;
import com.github.mpjct.jmpjct.mysql.proto.Packet;
import me.ele.jarch.athena.exception.AuthQuitException;
import me.ele.jarch.athena.exception.QuitException;

public class CmdAuthResultReal extends OnePacketCommand {

    public CmdAuthResultReal(byte[] packet) {
        super(packet);
    }

    @Override public boolean doInnerExecute() throws QuitException {
        if (Packet.getType(packet) != Flags.OK) {
            if (Packet.getType(this.packet) == Flags.ERR) {
                throw new AuthQuitException(ERR.loadFromPacket(this.packet).errorMessage);
            }
            throw new AuthQuitException("Failed to authentication.");
        }
        return true;
    }
}
