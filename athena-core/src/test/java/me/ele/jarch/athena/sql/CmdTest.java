package me.ele.jarch.athena.sql;

import com.github.mpjct.jmpjct.mysql.proto.Com_Ping;
import com.github.mpjct.jmpjct.mysql.proto.Com_Quit;
import com.github.mpjct.jmpjct.mysql.proto.Flags;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import me.ele.jarch.athena.allinone.DBConnectionInfo;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.scheduler.Scheduler;
import me.ele.jarch.athena.sharding.sql.Send2BatchCond;
import me.ele.jarch.athena.util.ZKCache;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Properties;

import static org.testng.Assert.*;

public class CmdTest {

    // CmdHandshakeReal not use sendBuf
    @Test(enabled = false) public void testCmdHandshake() throws QuitException {
        byte[] input = new byte[] {(byte) 0x4e, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x0a,
            (byte) 0x35, (byte) 0x2e, (byte) 0x36, (byte) 0x2e, (byte) 0x32, (byte) 0x31,
            (byte) 0x2d, (byte) 0x6c, (byte) 0x6f, (byte) 0x67, (byte) 0x00, (byte) 0x72,
            (byte) 0x6d, (byte) 0x5a, (byte) 0x00, (byte) 0x4e, (byte) 0x6c, (byte) 0x56,
            (byte) 0x4a, (byte) 0x4d, (byte) 0x26, (byte) 0x74, (byte) 0x38, (byte) 0x00,
            (byte) 0xdf, (byte) 0xf7, (byte) 0x21, (byte) 0x02, (byte) 0x00, (byte) 0x5f,
            (byte) 0x80, (byte) 0x15, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
            (byte) 0x58, (byte) 0x35, (byte) 0x3c, (byte) 0x6c, (byte) 0x2f, (byte) 0x42,
            (byte) 0x3e, (byte) 0x51, (byte) 0x5c, (byte) 0x5b, (byte) 0x32, (byte) 0x2a,
            (byte) 0x00, (byte) 0x6d, (byte) 0x79, (byte) 0x73, (byte) 0x71, (byte) 0x6c,
            (byte) 0x5f, (byte) 0x6e, (byte) 0x61, (byte) 0x74, (byte) 0x69, (byte) 0x76,
            (byte) 0x65, (byte) 0x5f, (byte) 0x70, (byte) 0x61, (byte) 0x73, (byte) 0x73,
            (byte) 0x77, (byte) 0x6f, (byte) 0x72, (byte) 0x64, (byte) 0x00};

        // remove some flags
        byte[] output =
            new byte[] {78, 0, 0, 0, 10, 53, 46, 54, 46, 50, 49, 45, 108, 111, 103, 0, 114, 109, 90,
                0, 78, 108, 86, 74, 77, 38, 116, 56, 0, 95, -9, 33, 2, 0, 95, -128, 21, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 88, 53, 60, 108, 47, 66, 62, 81, 92, 91, 50, 42, 0, 109, 121, 115,
                113, 108, 95, 110, 97, 116, 105, 118, 101, 95, 112, 97, 115, 115, 119, 111, 114,
                100, 0};

        CmdHandshakeReal auth = new CmdHandshakeReal(input);
        auth.execute();
        assertTrue(Arrays.equals(output, auth.sendBuf));
    }

    // CmdAuthFake will never use sendBuf
    @Test(enabled = false) public void testCmdAuthTest() throws QuitException {
        byte[] input = new byte[] {(byte) 0xbb, (byte) 0x00, (byte) 0x00, (byte) 0x01, (byte) 0x05,
            (byte) 0xa6, (byte) 0x7f, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
            (byte) 0x01, (byte) 0x21, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
            (byte) 0x00, (byte) 0x65, (byte) 0x6c, (byte) 0x65, (byte) 0x6d, (byte) 0x65,
            (byte) 0x00, (byte) 0x14, (byte) 0x49, (byte) 0xb4, (byte) 0xfe, (byte) 0xfd,
            (byte) 0x55, (byte) 0x7c, (byte) 0x03, (byte) 0xc8, (byte) 0xe5, (byte) 0x25,
            (byte) 0x74, (byte) 0x05, (byte) 0x5a, (byte) 0x4b, (byte) 0x8e, (byte) 0x6a,
            (byte) 0x3d, (byte) 0x4f, (byte) 0x79, (byte) 0xcf, (byte) 0x6d, (byte) 0x79,
            (byte) 0x73, (byte) 0x71, (byte) 0x6c, (byte) 0x5f, (byte) 0x6e, (byte) 0x61,
            (byte) 0x74, (byte) 0x69, (byte) 0x76, (byte) 0x65, (byte) 0x5f, (byte) 0x70,
            (byte) 0x61, (byte) 0x73, (byte) 0x73, (byte) 0x77, (byte) 0x6f, (byte) 0x72,
            (byte) 0x64, (byte) 0x00, (byte) 0x69, (byte) 0x03, (byte) 0x5f, (byte) 0x6f,
            (byte) 0x73, (byte) 0x08, (byte) 0x6f, (byte) 0x73, (byte) 0x78, (byte) 0x31,
            (byte) 0x30, (byte) 0x2e, (byte) 0x31, (byte) 0x30, (byte) 0x0c, (byte) 0x5f,
            (byte) 0x63, (byte) 0x6c, (byte) 0x69, (byte) 0x65, (byte) 0x6e, (byte) 0x74,
            (byte) 0x5f, (byte) 0x6e, (byte) 0x61, (byte) 0x6d, (byte) 0x65, (byte) 0x08,
            (byte) 0x6c, (byte) 0x69, (byte) 0x62, (byte) 0x6d, (byte) 0x79, (byte) 0x73,
            (byte) 0x71, (byte) 0x6c, (byte) 0x04, (byte) 0x5f, (byte) 0x70, (byte) 0x69,
            (byte) 0x64, (byte) 0x05, (byte) 0x33, (byte) 0x31, (byte) 0x31, (byte) 0x39,
            (byte) 0x37, (byte) 0x0f, (byte) 0x5f, (byte) 0x63, (byte) 0x6c, (byte) 0x69,
            (byte) 0x65, (byte) 0x6e, (byte) 0x74, (byte) 0x5f, (byte) 0x76, (byte) 0x65,
            (byte) 0x72, (byte) 0x73, (byte) 0x69, (byte) 0x6f, (byte) 0x6e, (byte) 0x06,
            (byte) 0x35, (byte) 0x2e, (byte) 0x36, (byte) 0x2e, (byte) 0x32, (byte) 0x34,
            (byte) 0x09, (byte) 0x5f, (byte) 0x70, (byte) 0x6c, (byte) 0x61, (byte) 0x74,
            (byte) 0x66, (byte) 0x6f, (byte) 0x72, (byte) 0x6d, (byte) 0x06, (byte) 0x78,
            (byte) 0x38, (byte) 0x36, (byte) 0x5f, (byte) 0x36, (byte) 0x34, (byte) 0x0c,
            (byte) 0x70, (byte) 0x72, (byte) 0x6f, (byte) 0x67, (byte) 0x72, (byte) 0x61,
            (byte) 0x6d, (byte) 0x5f, (byte) 0x6e, (byte) 0x61, (byte) 0x6d, (byte) 0x65,
            (byte) 0x05, (byte) 0x6d, (byte) 0x79, (byte) 0x73, (byte) 0x71, (byte) 0x6c};
        CmdAuthFake auth = new CmdAuthFake(input);
        auth.execute();
        assertTrue(Arrays.equals(input, auth.sendBuf));
    }

    @Test public void testCmdAuthResultOKTest() throws QuitException {
        byte[] input = new byte[] {(byte) 0x07, (byte) 0x00, (byte) 0x00, (byte) 0x02, (byte) 0x00,
            (byte) 0x00, (byte) 0x00, (byte) 0x02, (byte) 0x00, (byte) 0x00, (byte) 0x00};
        CmdAuthResultReal auth = new CmdAuthResultReal(input);
        auth.execute();
        assertTrue(auth.isDone());
    }

    @Test(expectedExceptions = QuitException.class) public void testCmdAuthResultFailTest()
        throws QuitException {
        byte[] input = new byte[] {(byte) 0x4d, (byte) 0x00, (byte) 0x00, (byte) 0x02, (byte) 0xff,
            (byte) 0x15, (byte) 0x04, (byte) 0x23, (byte) 0x32, (byte) 0x38, (byte) 0x30,
            (byte) 0x30, (byte) 0x30, (byte) 0x41, (byte) 0x63, (byte) 0x63, (byte) 0x65,
            (byte) 0x73, (byte) 0x73, (byte) 0x20, (byte) 0x64, (byte) 0x65, (byte) 0x6e,
            (byte) 0x69, (byte) 0x65, (byte) 0x64, (byte) 0x20, (byte) 0x66, (byte) 0x6f,
            (byte) 0x72, (byte) 0x20, (byte) 0x75, (byte) 0x73, (byte) 0x65, (byte) 0x72,
            (byte) 0x20, (byte) 0x27, (byte) 0x65, (byte) 0x6c, (byte) 0x65, (byte) 0x6d,
            (byte) 0x65, (byte) 0x31, (byte) 0x27, (byte) 0x40, (byte) 0x27, (byte) 0x31,
            (byte) 0x37, (byte) 0x32, (byte) 0x2e, (byte) 0x31, (byte) 0x36, (byte) 0x2e,
            (byte) 0x31, (byte) 0x30, (byte) 0x2e, (byte) 0x38, (byte) 0x32, (byte) 0x27,
            (byte) 0x20, (byte) 0x28, (byte) 0x75, (byte) 0x73, (byte) 0x69, (byte) 0x6e,
            (byte) 0x67, (byte) 0x20, (byte) 0x70, (byte) 0x61, (byte) 0x73, (byte) 0x73,
            (byte) 0x77, (byte) 0x6f, (byte) 0x72, (byte) 0x64, (byte) 0x3a, (byte) 0x20,
            (byte) 0x59, (byte) 0x45, (byte) 0x53, (byte) 0x29};
        CmdAuthResultReal auth = new CmdAuthResultReal(input);
        auth.execute();
    }

    @Test public void testCmdQueryTest() throws QuitException {
        byte[] input = new byte[] {(byte) 0x21, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x03,
            (byte) 0x73, (byte) 0x65, (byte) 0x6c, (byte) 0x65, (byte) 0x63, (byte) 0x74,
            (byte) 0x20, (byte) 0x40, (byte) 0x40, (byte) 0x76, (byte) 0x65, (byte) 0x72,
            (byte) 0x73, (byte) 0x69, (byte) 0x6f, (byte) 0x6e, (byte) 0x5f, (byte) 0x63,
            (byte) 0x6f, (byte) 0x6d, (byte) 0x6d, (byte) 0x65, (byte) 0x6e, (byte) 0x74,
            (byte) 0x20, (byte) 0x6c, (byte) 0x69, (byte) 0x6d, (byte) 0x69, (byte) 0x74,
            (byte) 0x20, (byte) 0x31};
        Send2BatchCond batchCond = new Send2BatchCond();

        CmdQuery query = new CmdQuery(input, "eosgroup");
        query.batchCond = batchCond;
        query.execute();
        assertTrue(query.isDone());
        assertEquals(query.query, "select @@version_comment limit 1");
    }

    @Test public void testCmdQueryResultTestReadServeralTimes() throws QuitException {
        byte[] input1 = new byte[] {(byte) 0x01, (byte) 0x00, (byte) 0x00, (byte) 0x01, (byte) 0x01,
            (byte) 0x27, (byte) 0x00, (byte) 0x00, (byte) 0x02, (byte) 0x03, (byte) 0x64,
            (byte) 0x65, (byte) 0x66, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x11,
            (byte) 0x40, (byte) 0x40, (byte) 0x76, (byte) 0x65, (byte) 0x72, (byte) 0x73,
            (byte) 0x69, (byte) 0x6f, (byte) 0x6e, (byte) 0x5f, (byte) 0x63, (byte) 0x6f,
            (byte) 0x6d, (byte) 0x6d, (byte) 0x65, (byte) 0x6e, (byte) 0x74, (byte) 0x00,
            (byte) 0x0c, (byte) 0x21, (byte) 0x00, (byte) 0x39, (byte) 0x00, (byte) 0x00,
            (byte) 0x00, (byte) 0xfd, (byte) 0x00, (byte) 0x00, (byte) 0x1f, (byte) 0x00,
            (byte) 0x00, (byte) 0x05, (byte) 0x00, (byte) 0x00, (byte) 0x03, (byte) 0xfe,};

        // byte[] input2 = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x02, (byte) 0x00, (byte) 0x14, (byte) 0x00, (byte) 0x00, (byte) 0x04, (byte) 0x13,
        // (byte) 0x53, (byte) 0x6f, (byte) 0x75, (byte) 0x72, (byte) 0x63, (byte) 0x65, (byte) 0x20, (byte) 0x64, (byte) 0x69, (byte) 0x73, (byte) 0x74,
        // (byte) 0x72, (byte) 0x69, (byte) 0x62, (byte) 0x75, (byte) 0x74, (byte) 0x69, (byte) 0x6f, (byte) 0x6e, (byte) 0x05, (byte) 0x00, (byte) 0x00,
        // (byte) 0x05, (byte) 0xfe, (byte) 0x00, (byte) 0x00, (byte) 0x02, (byte) 0x00 };

        QueryResultContext ctx = new QueryResultContext(false);
        ctx.packets = new LinkedList<byte[]>();
        // ctx.sendBuf = ByteBufAllocator.DEFAULT.directBuffer();
        ByteBuf input_buf = ByteBufAllocator.DEFAULT.directBuffer();
        input_buf.writeBytes(input1);
        CmdTcpPacket packet = new MySQLCmdTcpPacket(input_buf);
        boolean end = false;
        while (!end) {
            packet.execute();
            if (packet.isDone()) {
                ctx.packets.add(packet.packet);
                packet = new MySQLCmdTcpPacket(input_buf);
            } else {
                end = true;
            }
        }
    }

    @SuppressWarnings("unused") private Scheduler getScheduler() {
        DBConnectionInfo info = new DBConnectionInfo();
        Properties config = new Properties();
        config.setProperty(Constants.MAX_RESULT_BUF_SIZE, "10485760");
        config.setProperty(Constants.MAX_ACTIVE_DB_SESSIONS, "32");
        config.setProperty(Constants.MAX_QUEUE_SIZE, "5000");
        config.setProperty(Constants.REJECT_SQL_BY_PATTERN, "");
        config.setProperty(Constants.REJECT_SQL_BY_REGULAR_EXP, "");
        ZKCache zkCache = new ZKCache("channel");
        Scheduler scheduler = new Scheduler(info, zkCache, "");
        return scheduler;
    }

    @Test public void testPartialPacket() throws Exception {
        testPartialPacket(Flags.COM_QUIT, new Com_Quit().toPacket());
        testPartialPacket(Flags.COM_PING, new Com_Ping().toPacket());
    }

    private static void testPartialPacket(byte flag, byte[] expectedPackets) throws QuitException {
        ByteBuf buf = Unpooled.buffer();
        MySQLCmdTcpPacket packet = new MySQLCmdTcpPacket(buf);
        byte[] length = new byte[] {(byte) 0x01, (byte) 0x00, (byte) 0x00};
        byte[] sequenceId = new byte[] {(byte) 0x00};
        byte[] type = new byte[] {flag};
        buf.writeBytes(length);
        packet.execute();
        assertFalse(packet.isDone());
        buf.writeBytes(sequenceId);
        packet.execute();
        assertFalse(packet.isDone());
        buf.writeBytes(type);
        packet.execute();
        assertTrue(packet.isDone());
        assertEquals(buf.readableBytes(), 0);
        assertEquals(packet.getPacket(), expectedPackets);
    }
}
