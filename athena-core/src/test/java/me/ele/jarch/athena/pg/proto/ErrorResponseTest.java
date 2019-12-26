package me.ele.jarch.athena.pg.proto;

import me.ele.jarch.athena.pg.util.Severity;
import org.testng.annotations.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;

/**
 * Created by jinghao.wang on 16/11/24.
 */
public class ErrorResponseTest {
    private final byte[] packet =
        new byte[] {(byte) 0x45, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x6c, (byte) 0x53,
            (byte) 0x45, (byte) 0x52, (byte) 0x52, (byte) 0x4f, (byte) 0x52, (byte) 0x00,
            (byte) 0x56, (byte) 0x45, (byte) 0x52, (byte) 0x52, (byte) 0x4f, (byte) 0x52,
            (byte) 0x00, (byte) 0x43, (byte) 0x34, (byte) 0x32, (byte) 0x50, (byte) 0x30,
            (byte) 0x31, (byte) 0x00, (byte) 0x4d, (byte) 0x72, (byte) 0x65, (byte) 0x6c,
            (byte) 0x61, (byte) 0x74, (byte) 0x69, (byte) 0x6f, (byte) 0x6e, (byte) 0x20,
            (byte) 0x22, (byte) 0x64, (byte) 0x61, (byte) 0x6c, (byte) 0x74, (byte) 0x65,
            (byte) 0x73, (byte) 0x74, (byte) 0x64, (byte) 0x62, (byte) 0x22, (byte) 0x20,
            (byte) 0x64, (byte) 0x6f, (byte) 0x65, (byte) 0x73, (byte) 0x20, (byte) 0x6e,
            (byte) 0x6f, (byte) 0x74, (byte) 0x20, (byte) 0x65, (byte) 0x78, (byte) 0x69,
            (byte) 0x73, (byte) 0x74, (byte) 0x00, (byte) 0x50, (byte) 0x31, (byte) 0x37,
            (byte) 0x00, (byte) 0x46, (byte) 0x70, (byte) 0x61, (byte) 0x72, (byte) 0x73,
            (byte) 0x65, (byte) 0x5f, (byte) 0x72, (byte) 0x65, (byte) 0x6c, (byte) 0x61,
            (byte) 0x74, (byte) 0x69, (byte) 0x6f, (byte) 0x6e, (byte) 0x2e, (byte) 0x63,
            (byte) 0x00, (byte) 0x4c, (byte) 0x31, (byte) 0x31, (byte) 0x35, (byte) 0x39,
            (byte) 0x00, (byte) 0x52, (byte) 0x70, (byte) 0x61, (byte) 0x72, (byte) 0x73,
            (byte) 0x65, (byte) 0x72, (byte) 0x4f, (byte) 0x70, (byte) 0x65, (byte) 0x6e,
            (byte) 0x54, (byte) 0x61, (byte) 0x62, (byte) 0x6c, (byte) 0x65, (byte) 0x00,
            (byte) 0x00};

    @Test public void testToPacket() {
        ErrorResponse errorResponse =
            new ErrorResponse(Severity.ERROR, "42P01", "relation \"daltestdb\" does not exist",
                "parse_relation.c", "1159", "parserOpenTable");
        errorResponse.addField(PGFlags.SEVERITY_V9_6, "ERROR");
        errorResponse.addField((byte) 'P', "17");
        ErrorResponse expectedErrorResponse = ErrorResponse.loadFromPacket(packet);
        assertEquals(errorResponse, expectedErrorResponse);
    }

    @Test public void testLoadFromPacket() throws Exception {
        ErrorResponse errorResponse = ErrorResponse.loadFromPacket(packet);
        Map<Byte, String> expected = new LinkedHashMap<>();
        expected.put((byte) 'S', "ERROR");
        expected.put((byte) 'V', "ERROR");
        expected.put((byte) 'C', "42P01");
        expected.put((byte) 'M', "relation \"daltestdb\" does not exist");
        expected.put((byte) 'P', "17");
        expected.put((byte) 'F', "parse_relation.c");
        expected.put((byte) 'L', "1159");
        expected.put((byte) 'R', "parserOpenTable");
        assertEquals(errorResponse.getFieldWithValue(), expected);
    }
}
