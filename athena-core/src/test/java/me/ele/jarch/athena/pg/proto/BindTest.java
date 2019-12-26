package me.ele.jarch.athena.pg.proto;

import me.ele.jarch.athena.pg.proto.Bind.Param;
import me.ele.jarch.athena.pg.proto.Bind.ResultFormat;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author shaoyang.qi
 */
public class BindTest {
    @Test public void testParamToPacket() {
        byte[] paramVals = "1dal".getBytes();
        Param param = new Param(paramVals);
        byte[] paramBytes = param.toPacket();

        int expectedDataTotalLen = 4 + paramVals.length;
        assertEquals(paramBytes.length, expectedDataTotalLen);
        int expectedDataPayloadLen = expectedDataTotalLen - 4;
        PGProto proto = new PGProto(paramBytes);
        assertEquals(proto.readInt32(), expectedDataPayloadLen);
        assertEquals(proto.readBytes(expectedDataPayloadLen), paramVals);
    }

    @Test public void testResultFormatToPacket() {
        int[] resultFormatVals = new int[] {0, 0, 0, 1};
        ResultFormat format = new ResultFormat(resultFormatVals);
        byte[] bytes = format.toPacket();

        int expectedDataTotalLen = 2 + 2 * resultFormatVals.length;
        assertEquals(bytes.length, expectedDataTotalLen);
        int expectedValCount = resultFormatVals.length;
        PGProto proto = new PGProto(bytes);
        assertEquals(proto.readInt16(), expectedValCount);
        for (int i = 0; i < expectedValCount; i++) {
            assertEquals(proto.readInt16(), resultFormatVals[i]);
        }
    }

    @Test public void testToPacket() {
        String portal = "";
        String statement = "py:0x103d9a9b0";
        int[] paramFormatIds = new int[] {1, 1};
        Param[] params = new Param[] {new Param(new byte[] {0x01}), new Param("1dal".getBytes())};
        int[] resultFormatVals = new int[] {};
        ResultFormat format = new ResultFormat(resultFormatVals);
        Bind bind = new Bind();
        bind.setPortal(portal);
        bind.setStatement(statement);
        bind.setParamFormatIds(paramFormatIds);
        bind.setParams(params);
        bind.setResultFormat(format);

        int expectedParamsLen = 0;
        for (Param param : params) {
            expectedParamsLen += param.toPacket().length;
        }
        byte[] formatBytes = format.toPacket();
        byte[] bindBytes = bind.toPacket();

        int expectedDataTotalLen =
            1 + 4 + portal.length() + 1 + statement.length() + 1 + 2 + 2 * paramFormatIds.length + 2
                + expectedParamsLen + formatBytes.length;
        assertEquals(bindBytes.length, expectedDataTotalLen);

        PGProto proto = new PGProto(bindBytes);

        byte expectedDataType = PGFlags.C_BIND;
        assertEquals(proto.readByte(), expectedDataType);

        int expectedDataPayloadLen = expectedDataTotalLen - 1;
        assertEquals(proto.readInt32(), expectedDataPayloadLen);

        assertEquals(proto.readNullStr(), portal);
        assertEquals(proto.readNullStr(), statement);
        int expectedParamFormatIdCount = paramFormatIds.length;
        assertEquals(proto.readInt16(), expectedParamFormatIdCount);
        for (int i = 0; i < expectedParamFormatIdCount; i++) {
            assertEquals(proto.readInt16(), paramFormatIds[i]);
        }
        int expectedParamCount = params.length;
        assertEquals(proto.readInt16(), expectedParamCount);
        for (int i = 0; i < expectedParamCount; i++) {
            Param param = params[i];
            assertEquals(proto.readInt32(), param.getParamLen());
            assertEquals(proto.readBytes(param.getParamLen()), param.getParamVals());
        }
        int expectedFormatCount = format.getResultFormatCount();
        assertEquals(proto.readInt16(), expectedFormatCount);
        int[] expectedResultFormatVals = format.getResultFormatVals();
        for (int i = 0; i < expectedFormatCount; i++) {
            assertEquals(proto.readInt16(), expectedResultFormatVals[i]);
        }
    }

    @Test public void testLoadFromPacket() {
        String portal = "";
        String statement = "py:0x103d9a9b0";
        int[] paramFormatIds = new int[] {};
        Param[] params = new Param[] {};
        int[] resultFormatVals = new int[] {0, 0, 0, 1};
        ResultFormat format = new ResultFormat(resultFormatVals);
        Bind bind = new Bind();
        bind.setPortal(portal);
        bind.setStatement(statement);
        bind.setParamFormatIds(paramFormatIds);
        bind.setParams(params);
        bind.setResultFormat(format);
        byte[] bindBytes = bind.toPacket();

        Bind newBind = Bind.loadFromPacket(bindBytes);

        assertEquals(newBind.toPacket(), bindBytes);
        assertEquals(newBind.getPortal(), bind.getPortal());
        assertEquals(newBind.getStatement(), bind.getStatement());
        assertEquals(newBind.getParamFormatIdCount(), bind.getParamFormatIdCount());
        assertEquals(newBind.getParamFormatIds(), bind.getParamFormatIds());
        assertEquals(newBind.getParamCount(), bind.getParamCount());
        Param[] newParams = newBind.getParams();
        Param[] expectedParams = bind.getParams();
        for (int i = 0; i < expectedParams.length; i++) {
            assertEquals(newParams[i].getParamLen(), expectedParams[i].getParamLen());
            assertEquals(newParams[i].getParamVals(), expectedParams[i].getParamVals());
        }
        ResultFormat newFormat = newBind.getResultFormat();
        assertEquals(newFormat.getResultFormatCount(), format.getResultFormatCount());
        assertEquals(newFormat.getResultFormatVals(), format.getResultFormatVals());
    }
}
