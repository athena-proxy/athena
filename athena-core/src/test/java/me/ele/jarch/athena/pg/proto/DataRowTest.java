package me.ele.jarch.athena.pg.proto;

import me.ele.jarch.athena.pg.proto.DataRow.PGCol;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * @author shaoyang.qi
 */
public class DataRowTest {
    @Test public void testPGColToPacket() {
        byte[] data = "dataRow".getBytes();
        PGCol pgCol = new PGCol(data);
        // test PGCol
        byte[] colBytes = pgCol.toPacket();
        int expectedDataTotalLen = 4 + data.length;
        assertEquals(colBytes.length, expectedDataTotalLen);
        int expectedDataPayloadLen = expectedDataTotalLen - 4;
        PGProto proto = new PGProto(colBytes);
        assertEquals(proto.readInt32(), expectedDataPayloadLen);
        assertEquals(proto.readBytes(expectedDataPayloadLen), data);
    }

    @Test public void testToPacket() {
        byte[] idData = "1".getBytes();
        PGCol idPgCol = new PGCol(idData);

        byte[] nameData = "dataRow".getBytes();
        PGCol namePgCol = new PGCol(nameData);

        PGCol[] pgCols = new PGCol[] {idPgCol, namePgCol};
        DataRow dataRow = new DataRow(pgCols);

        byte[] idColBytes = idPgCol.toPacket();
        byte[] nameColBytes = namePgCol.toPacket();
        byte[] dataRowBytes = dataRow.toPacket();

        int expectedDataTotalLen = 1 + 4 + 2 + idColBytes.length + nameColBytes.length;
        assertEquals(dataRowBytes.length, expectedDataTotalLen);

        PGProto proto = new PGProto(dataRowBytes);

        byte expectedDataType = PGFlags.DATA_ROW;
        assertEquals(proto.readByte(), expectedDataType);

        int expectedDataPayloadLen = expectedDataTotalLen - 1;
        assertEquals(proto.readInt32(), expectedDataPayloadLen);

        int expectedColCount = 2;
        assertEquals(proto.readInt16(), expectedColCount);

        assertEquals(proto.readBytes(idColBytes.length), idColBytes);
        assertEquals(proto.readBytes(nameColBytes.length), nameColBytes);
    }

    @Test public void testLoadFromPacket() {
        byte[] nameData = "dataRow".getBytes();
        PGCol namePgCol = new PGCol(nameData);

        PGCol[] pgCols = new PGCol[] {namePgCol};
        DataRow dataRow = new DataRow(pgCols);
        byte[] dataRowBytes = dataRow.toPacket();

        DataRow newDataRow = DataRow.loadFromPacket(dataRowBytes);

        assertEquals(newDataRow.toPacket(), dataRowBytes);
        assertEquals(newDataRow.getColCount(), dataRow.getColCount());

        PGCol[] newPgCols = newDataRow.getCols();
        assertNotNull(newPgCols);
        assertEquals(newPgCols.length, pgCols.length);

        PGCol newNamePgCol = newPgCols[0];
        assertEquals(newNamePgCol.getDataLen(), namePgCol.getDataLen());
        assertEquals(newNamePgCol.getData(), nameData);
    }
}
