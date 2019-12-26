package com.github.mpjct.jmpjct.mysql.proto;

import me.ele.jarch.athena.util.proto.ResponseClassifier;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;

/**
 * Created by jinghao.wang on 2017/8/1.
 */
public class MysqlResponseClassifierTest {
    /*MySQL Protocol
    Packet Length: 73
    Packet Number: 2
    Catalog: def
    Database: daltestdb
    Table: dal_sequences
    Original table: dal_sequences
    Name: seq_name
    Original name: seq_name
    Charset number: utf8 COLLATE utf8_general_ci (33)
    Length: 192
    Type: FIELD_TYPE_VAR_STRING (253)
    Flags: 0x4005
    Decimals: 0*/
    private byte[] colDef =
        {0x49, 0x00, 0x00, 0x02, 0x03, 0x64, 0x65, 0x66, 0x09, 0x64, 0x61, 0x6c, 0x74, 0x65, 0x73,
            0x74, 0x64, 0x62, 0x0d, 0x64, 0x61, 0x6c, 0x5f, 0x73, 0x65, 0x71, 0x75, 0x65, 0x6e,
            0x63, 0x65, 0x73, 0x0d, 0x64, 0x61, 0x6c, 0x5f, 0x73, 0x65, 0x71, 0x75, 0x65, 0x6e,
            0x63, 0x65, 0x73, 0x08, 0x73, 0x65, 0x71, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x08, 0x73,
            0x65, 0x71, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x0c, 0x21, 0x00, (byte) 0xc0, 0x00, 0x00,
            0x00, (byte) 0xfd, 0x05, 0x40, 0x00, 0x00, 0x00};

    /*MySQL Protocol
    Packet Length: 29
    Packet Number: 4
    text: apollo_tracking_shard_id_dev*/
    private byte[] row =
        {(byte) 0x1d, (byte) 0x00, (byte) 0x00, (byte) 0x04, (byte) 0x1c, (byte) 0x61, (byte) 0x70,
            (byte) 0x6f, (byte) 0x6c, (byte) 0x6c, (byte) 0x6f, (byte) 0x5f, (byte) 0x74,
            (byte) 0x72, (byte) 0x61, (byte) 0x63, (byte) 0x6b, (byte) 0x69, (byte) 0x6e,
            (byte) 0x67, (byte) 0x5f, (byte) 0x73, (byte) 0x68, (byte) 0x61, (byte) 0x72,
            (byte) 0x64, (byte) 0x5f, (byte) 0x69, (byte) 0x64, (byte) 0x5f, (byte) 0x64,
            (byte) 0x65, (byte) 0x76};

    @Test public void testToPackets() throws Exception {
        ResponseClassifier responseClassifier = new MysqlResponseClassifier();
        responseClassifier.addColumn(colDef);
        responseClassifier.addRow(row);

        List<byte[]> toPackets = responseClassifier.toPackets();

        // assert colCount
        ColCount colColunt = ColCount.loadFromPacket(toPackets.get(0));
        assertEquals(colColunt.sequenceId, 1);
        assertEquals(colColunt.colCount, 1);

        // assert column def
        Column column = Column.loadFromPacket(toPackets.get(1));
        assertEquals(column.catalog, "def");
        assertEquals(column.table, "dal_sequences");
        assertEquals(column.org_table, "dal_sequences");
        assertEquals(column.name, "seq_name");
        assertEquals(column.org_name, "seq_name");
        assertEquals(column.characterSet, 33);
        assertEquals(column.type, 253);
        assertEquals(column.decimals, 0);

        // assert eof
        assertEquals(Packet.getType(toPackets.get(2)), Flags.EOF);

        // assert data row
        Row row = Row.loadFromPacket(toPackets.get(3), 1);
        assertEquals(row.data.size(), 1);
        assertEquals((String) row.data.get(0), "apollo_tracking_shard_id_dev");

        // assert eof
        assertEquals(Packet.getType(toPackets.get(4)), Flags.EOF);
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "unsupport call `\\w+` during MySQL protocol")
    public void testCallUnsuppportGetCommandComplete() throws Exception {
        ResponseClassifier responseClassifier = new MysqlResponseClassifier();
        responseClassifier.getCommandComplete();
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "unsupport call `\\w+` during MySQL protocol")
    public void testCallUnsuppportSetCommandComplete() throws Exception {
        ResponseClassifier responseClassifier = new MysqlResponseClassifier();
        responseClassifier.setCommandComplete(new byte[0]);
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "unsupport call `\\w+` during MySQL protocol")
    public void testCallUnsuppportGetReadyForQuery() throws Exception {
        ResponseClassifier responseClassifier = new MysqlResponseClassifier();
        responseClassifier.getReadyForQuery();
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "unsupport call `\\w+` during MySQL protocol")
    public void testCallUnsuppportSetReadyForQuery() throws Exception {
        ResponseClassifier responseClassifier = new MysqlResponseClassifier();
        responseClassifier.setReadyForQuery(new byte[0]);
    }
}
