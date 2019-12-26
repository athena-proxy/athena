package me.ele.jarch.athena.netty;

import me.ele.jarch.athena.sql.CmdTcpPacket;
import me.ele.jarch.athena.sql.MySQLCmdTcpPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class MySqlServerPacketDecoder extends SqlServerPacketDecoder {
    private static final Logger logger = LoggerFactory.getLogger(MySqlServerPacketDecoder.class);

    @Override protected CmdTcpPacket newPacket() {
        return new MySQLCmdTcpPacket(inputbuf);
    }

    @Override protected void recvPacketWhenNoSqlCtxBind() {
        packets.forEach(p -> logger.error(
            "strange! UnExpected serverChannel HashCode=,packets=" + Arrays.toString(p) + ",conn="
                + serverSession.getOriginDbConnInfo()));
        packets.clear();
    }
}
