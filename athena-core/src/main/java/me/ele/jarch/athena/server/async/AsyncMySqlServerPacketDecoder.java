package me.ele.jarch.athena.server.async;

import me.ele.jarch.athena.sql.CmdTcpPacket;
import me.ele.jarch.athena.sql.MySQLCmdTcpPacket;

public class AsyncMySqlServerPacketDecoder extends AsyncSqlServerPacketDecoder {

    @Override protected CmdTcpPacket newPacket() {
        return new MySQLCmdTcpPacket(inputbuf);
    }
}
