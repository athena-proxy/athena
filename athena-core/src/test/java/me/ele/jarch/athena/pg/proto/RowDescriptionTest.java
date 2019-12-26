package me.ele.jarch.athena.pg.proto;

import me.ele.jarch.athena.pg.proto.RowDescription.PGColumn;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * @author shaoyang.qi
 */
public class RowDescriptionTest {
    private PGColumn getIdColumn() {
        String col = "id";
        int tableOid = 16387;
        int colNo = 1;
        int typeOid = 23;
        int typeLen = 4;
        int typMod = -1;
        int format = 0;
        PGColumn column = new PGColumn();
        column.setName(col);
        column.setTableOid(tableOid);
        column.setColNo(colNo);
        column.setTypeOid(typeOid);
        column.setTypeLen(typeLen);
        column.setTypeMod(typMod);
        column.setFormat(format);
        return column;
    }

    private PGColumn getNameColumn() {
        String col = "name";
        int tableOid = 16387;
        int colNo = 2;
        int typeOid = 1043;
        int typeLen = -1;
        int typMod = 132;
        int format = 0;
        PGColumn column = new PGColumn();
        column.setName(col);
        column.setTableOid(tableOid);
        column.setColNo(colNo);
        column.setTypeOid(typeOid);
        column.setTypeLen(typeLen);
        column.setTypeMod(typMod);
        column.setFormat(format);
        return column;
    }

    @Test public void testPGColumnToPacket() {
        PGColumn column = getNameColumn();
        byte[] bytes = column.toPacket();

        int expectedTotalLen = column.getName().length() + 1 + 4 + 2 + 4 + 2 + 4 + 2;
        assertEquals(bytes.length, expectedTotalLen);
        PGProto proto = new PGProto(bytes);
        assertEquals(proto.readNullStr(), column.getName());
        assertEquals(proto.readInt32(), column.getTableOid());
        assertEquals(proto.readInt16(), column.getColNo());
        assertEquals(proto.readInt32(), column.getTypeOid());
        assertEquals(proto.readInt16(), column.getTypeLen());
        assertEquals(proto.readInt32(), column.getTypeMod());
        assertEquals(proto.readInt16(), column.getFormat());
    }

    @Test public void testToPacket() {
        PGColumn idColumn = getIdColumn();
        PGColumn nameColumn = getNameColumn();
        PGColumn[] columns = new PGColumn[] {idColumn, nameColumn};
        RowDescription rowDesc = new RowDescription(columns);

        byte[] idBytes = idColumn.toPacket();
        byte[] nameBytes = nameColumn.toPacket();
        byte[] bytes = rowDesc.toPacket();

        int expectedDataTotalLen = 1 + 4 + 2 + idBytes.length + nameBytes.length;
        assertEquals(bytes.length, expectedDataTotalLen);

        PGProto proto = new PGProto(bytes);

        byte expectedDataType = PGFlags.ROW_DESCRIPTION;
        assertEquals(proto.readByte(), expectedDataType);

        int expectedDataPayloadLen = expectedDataTotalLen - 1;
        assertEquals(proto.readInt32(), expectedDataPayloadLen);

        int expectedColCount = 2;
        assertEquals(proto.readInt16(), expectedColCount);

        assertEquals(proto.readBytes(idBytes.length), idBytes);
        assertEquals(proto.readBytes(nameBytes.length), nameBytes);
    }

    @Test public void testPGColumnLoadFromPacket() {
        PGColumn column = getIdColumn();
        byte[] bytes = column.toPacket();

        PGColumn newColumn = PGColumn.loadFromPacket(bytes);
        assertEquals(newColumn.toPacket(), bytes);
        assertEquals(newColumn.getName(), column.getName());
        assertEquals(newColumn.getTableOid(), column.getTableOid());
        assertEquals(newColumn.getColNo(), column.getColNo());
        assertEquals(newColumn.getTypeOid(), column.getTypeOid());
        assertEquals(newColumn.getTypeLen(), column.getTypeLen());
        assertEquals(newColumn.getTypeMod(), column.getTypeMod());
        assertEquals(newColumn.getFormat(), column.getFormat());
    }

    @Test public void testLoadFromPacket() {
        PGColumn idColumn = getIdColumn();
        PGColumn[] columns = new PGColumn[] {idColumn};
        RowDescription rowDesc = new RowDescription(columns);
        byte[] bytes = rowDesc.toPacket();

        RowDescription newRowDesc = RowDescription.loadFromPacket(bytes);
        assertEquals(newRowDesc.toPacket(), bytes);
        assertEquals(newRowDesc.getColCount(), rowDesc.getColCount());

        PGColumn[] newColumns = newRowDesc.getColumns();
        assertNotNull(newColumns);
        assertEquals(newColumns.length, columns.length);

        PGColumn newIdColumn = newColumns[0];
        assertEquals(newIdColumn.toPacket(), idColumn.toPacket());
        assertEquals(newIdColumn.getName(), idColumn.getName());
        assertEquals(newIdColumn.getTableOid(), idColumn.getTableOid());
        assertEquals(newIdColumn.getColNo(), idColumn.getColNo());
        assertEquals(newIdColumn.getTypeOid(), idColumn.getTypeOid());
        assertEquals(newIdColumn.getTypeLen(), idColumn.getTypeLen());
        assertEquals(newIdColumn.getTypeMod(), idColumn.getTypeMod());
        assertEquals(newIdColumn.getFormat(), idColumn.getFormat());
    }
}
