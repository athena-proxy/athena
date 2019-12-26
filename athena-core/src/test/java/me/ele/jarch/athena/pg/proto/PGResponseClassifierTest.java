package me.ele.jarch.athena.pg.proto;

import me.ele.jarch.athena.util.proto.ResponseClassifier;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;

/**
 * Created by jinghao.wang on 2017/8/1.
 */
public class PGResponseClassifierTest {
    /*PostgreSQL
    Type: Row description
    Length: 33
    Field count: 1
    Column name: order_id
    Table OID: 16419
    Column index: 3
    Type OID: 20
    Column length: 8
    Type modifier: -1
    Format: Text (0)*/
    private byte[] rowDesc =
        {(byte) 0x54, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x21, (byte) 0x00, (byte) 0x01,
            (byte) 0x6f, (byte) 0x72, (byte) 0x64, (byte) 0x65, (byte) 0x72, (byte) 0x5f,
            (byte) 0x69, (byte) 0x64, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x40,
            (byte) 0x23, (byte) 0x00, (byte) 0x03, (byte) 0x00, (byte) 0x00, (byte) 0x00,
            (byte) 0x14, (byte) 0x00, (byte) 0x08, (byte) 0xff, (byte) 0xff, (byte) 0xff,
            (byte) 0xff, (byte) 0x00, (byte) 0x00};
    /*PostgreSQL
    Type: Data row
    Length: 17
    Field count: 1
    Column length: 7
    Data: 32363331323332*/
    private byte[] dataRow =
        {(byte) 0x44, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x11, (byte) 0x00, (byte) 0x01,
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x07, (byte) 0x32, (byte) 0x36,
            (byte) 0x33, (byte) 0x31, (byte) 0x32, (byte) 0x33, (byte) 0x32};

    @Test public void testToPackets() throws Exception {
        ResponseClassifier responseClassifier = new PGResponseClassifier();
        responseClassifier.addColumn(rowDesc);
        responseClassifier.addRow(dataRow);
        responseClassifier.setCommandComplete(new CommandComplete("SELECT 1").toPacket());
        responseClassifier.setReadyForQuery(ReadyForQuery.IDLE.toPacket());

        List<byte[]> toPackets = responseClassifier.toPackets();

        assertEquals(toPackets.size(), 4);

        // assert rowDesc
        RowDescription rowDescription = RowDescription.loadFromPacket(toPackets.get(0));
        assertEquals(PGMessage.getType(toPackets.get(0)), PGFlags.ROW_DESCRIPTION);
        assertEquals(rowDescription.getColCount(), 1);
        RowDescription.PGColumn column = rowDescription.getColumns()[0];
        assertEquals(column.getName(), "order_id");
        assertEquals(column.getTypeOid(), 20);
        assertEquals(column.getColNo(), 3);
        assertEquals(column.getFormat(), 0);
        assertEquals(column.getTypeLen(), 8);

        assertEquals(PGMessage.getType(toPackets.get(1)), PGFlags.DATA_ROW);
        assertEquals(PGMessage.getType(toPackets.get(2)), PGFlags.COMMAND_COMPLETE);
        assertEquals(PGMessage.getType(toPackets.get(3)), PGFlags.READY_FOR_QUERY);
    }

}
