package me.ele.jarch.athena.pg.proto;

import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Created by jinghao.wang on 16/11/25.
 */
public class StartupMessageTest {
    private final byte[] withDatabasePacket =
        new byte[] {(byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x25, (byte) 0x00, (byte) 0x03,
            (byte) 0x00, (byte) 0x00, (byte) 0x75, (byte) 0x73, (byte) 0x65, (byte) 0x72,
            (byte) 0x00, (byte) 0x72, (byte) 0x6f, (byte) 0x6f, (byte) 0x74, (byte) 0x00,
            (byte) 0x64, (byte) 0x61, (byte) 0x74, (byte) 0x61, (byte) 0x62, (byte) 0x61,
            (byte) 0x73, (byte) 0x65, (byte) 0x00, (byte) 0x70, (byte) 0x6f, (byte) 0x73,
            (byte) 0x74, (byte) 0x67, (byte) 0x72, (byte) 0x65, (byte) 0x73, (byte) 0x00,
            (byte) 0x00};
    private final byte[] withoutDBPacket =
        new byte[] {(byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x21, (byte) 0x00, (byte) 0x03,
            (byte) 0x00, (byte) 0x00, (byte) 0x75, (byte) 0x73, (byte) 0x65, (byte) 0x72,
            (byte) 0x00, (byte) 0x72, (byte) 0x6f, (byte) 0x6f, (byte) 0x74, (byte) 0x00,
            (byte) 0x64, (byte) 0x61, (byte) 0x74, (byte) 0x61, (byte) 0x62, (byte) 0x61,
            (byte) 0x73, (byte) 0x65, (byte) 0x00, (byte) 0x72, (byte) 0x6f, (byte) 0x6f,
            (byte) 0x74, (byte) 0x00, (byte) 0x00};
    private final byte[] withArgsPacket =
        new byte[] {(byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x5d, (byte) 0x00, (byte) 0x03,
            (byte) 0x00, (byte) 0x00, (byte) 0x75, (byte) 0x73, (byte) 0x65, (byte) 0x72,
            (byte) 0x00, (byte) 0x72, (byte) 0x6f, (byte) 0x6f, (byte) 0x74, (byte) 0x00,
            (byte) 0x64, (byte) 0x61, (byte) 0x74, (byte) 0x61, (byte) 0x62, (byte) 0x61,
            (byte) 0x73, (byte) 0x65, (byte) 0x00, (byte) 0x70, (byte) 0x67, (byte) 0x74,
            (byte) 0x65, (byte) 0x73, (byte) 0x74, (byte) 0x64, (byte) 0x62, (byte) 0x00,
            (byte) 0x61, (byte) 0x70, (byte) 0x70, (byte) 0x6c, (byte) 0x69, (byte) 0x63,
            (byte) 0x61, (byte) 0x74, (byte) 0x69, (byte) 0x6f, (byte) 0x6e, (byte) 0x5f,
            (byte) 0x6e, (byte) 0x61, (byte) 0x6d, (byte) 0x65, (byte) 0x00, (byte) 0x50,
            (byte) 0x6f, (byte) 0x73, (byte) 0x74, (byte) 0x69, (byte) 0x63, (byte) 0x6f,
            (byte) 0x20, (byte) 0x31, (byte) 0x2e, (byte) 0x30, (byte) 0x2e, (byte) 0x31,
            (byte) 0x30, (byte) 0x00, (byte) 0x63, (byte) 0x6c, (byte) 0x69, (byte) 0x65,
            (byte) 0x6e, (byte) 0x74, (byte) 0x5f, (byte) 0x65, (byte) 0x6e, (byte) 0x63,
            (byte) 0x6f, (byte) 0x64, (byte) 0x69, (byte) 0x6e, (byte) 0x67, (byte) 0x00,
            (byte) 0x55, (byte) 0x4e, (byte) 0x49, (byte) 0x43, (byte) 0x4f, (byte) 0x44,
            (byte) 0x45, (byte) 0x00, (byte) 0x00};
    private final byte[] virtualWithArgsPacket =
        new byte[] {PGFlags.C_STARTUP_MESSAGE, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x5d,
            (byte) 0x00, (byte) 0x03, (byte) 0x00, (byte) 0x00, (byte) 0x75, (byte) 0x73,
            (byte) 0x65, (byte) 0x72, (byte) 0x00, (byte) 0x72, (byte) 0x6f, (byte) 0x6f,
            (byte) 0x74, (byte) 0x00, (byte) 0x64, (byte) 0x61, (byte) 0x74, (byte) 0x61,
            (byte) 0x62, (byte) 0x61, (byte) 0x73, (byte) 0x65, (byte) 0x00, (byte) 0x70,
            (byte) 0x67, (byte) 0x74, (byte) 0x65, (byte) 0x73, (byte) 0x74, (byte) 0x64,
            (byte) 0x62, (byte) 0x00, (byte) 0x61, (byte) 0x70, (byte) 0x70, (byte) 0x6c,
            (byte) 0x69, (byte) 0x63, (byte) 0x61, (byte) 0x74, (byte) 0x69, (byte) 0x6f,
            (byte) 0x6e, (byte) 0x5f, (byte) 0x6e, (byte) 0x61, (byte) 0x6d, (byte) 0x65,
            (byte) 0x00, (byte) 0x50, (byte) 0x6f, (byte) 0x73, (byte) 0x74, (byte) 0x69,
            (byte) 0x63, (byte) 0x6f, (byte) 0x20, (byte) 0x31, (byte) 0x2e, (byte) 0x30,
            (byte) 0x2e, (byte) 0x31, (byte) 0x30, (byte) 0x00, (byte) 0x63, (byte) 0x6c,
            (byte) 0x69, (byte) 0x65, (byte) 0x6e, (byte) 0x74, (byte) 0x5f, (byte) 0x65,
            (byte) 0x6e, (byte) 0x63, (byte) 0x6f, (byte) 0x64, (byte) 0x69, (byte) 0x6e,
            (byte) 0x67, (byte) 0x00, (byte) 0x55, (byte) 0x4e, (byte) 0x49, (byte) 0x43,
            (byte) 0x4f, (byte) 0x44, (byte) 0x45, (byte) 0x00, (byte) 0x00};

    @Test public void testWithoutDatabase() {

        StartupMessage startupMessage = new StartupMessage("root");
        assertTrue(Arrays.equals(startupMessage.toPacket(), withoutDBPacket));
    }

    @Test public void testWithDatabasee() {
        StartupMessage startupMessage = new StartupMessage("root", "postgres");
        assertTrue(Arrays.equals(startupMessage.toPacket(), withDatabasePacket));
    }

    @Test public void testGetArgs() throws Exception {
        Map<String, String> expectArgs = new LinkedHashMap<>();
        expectArgs.put("application_name", "Postico 1.0.10");
        expectArgs.put("client_encoding", "UNICODE");
        StartupMessage startupMessage = StartupMessage.loadFromPacket(withArgsPacket);
        assertEquals(startupMessage.getArgs(), expectArgs);
    }

    @Test public void testAddArgs() {
        StartupMessage startupMessage = new StartupMessage("root", "pgtestdb");
        startupMessage.addArgs("application_name", "Postico 1.0.10");
        startupMessage.addArgs("client_encoding", "UNICODE");
        assertTrue(Arrays.equals(startupMessage.toPacket(), withArgsPacket));
    }

    @Test public void testGetUser() throws Exception {
        StartupMessage startupMessage = StartupMessage.loadFromPacket(withDatabasePacket);
        String expectedUser = "root";
        assertEquals(startupMessage.getUser(), expectedUser);
    }

    @Test public void testGetDatabase() throws Exception {
        StartupMessage startupMessage = StartupMessage.loadFromPacket(withDatabasePacket);
        String expectedDatabase = "postgres";
        assertEquals(startupMessage.getDatabase(), expectedDatabase);

        StartupMessage startupMessage1 = StartupMessage.loadFromPacket(withoutDBPacket);
        String expectedDatabase1 = "root";
        assertEquals(startupMessage1.getDatabase(), expectedDatabase1);
    }

    @Test public void testLoadFromVirtualPacket() {
        StartupMessage startupMessage = StartupMessage.loadFromVirtualPacket(virtualWithArgsPacket);
        String expectedUser = "root";
        assertEquals(startupMessage.getUser(), expectedUser);
        String expectedDatabase = "pgtestdb";
        assertEquals(startupMessage.getDatabase(), expectedDatabase);
        Map<String, String> expectArgs = new LinkedHashMap<>();
        expectArgs.put("application_name", "Postico 1.0.10");
        expectArgs.put("client_encoding", "UNICODE");
        assertEquals(startupMessage.getArgs(), expectArgs);

        assertEquals(startupMessage.getTypeByte(), PGFlags.C_STARTUP_MESSAGE);
    }
}
