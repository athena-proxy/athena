package me.ele.jarch.athena.sharding;

import me.ele.jarch.athena.pg.proto.*;
import me.ele.jarch.athena.util.AggregateFunc;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by jinghao.wang on 2017/8/1.
 */
public class PGShardingResultSetTest {
    private List<AggregateFunc> columnAggrTypes = new ArrayList<>();
    private RowDescription rowDescription = new RowDescription();
    private DataRow dataRow1 = new DataRow();
    private DataRow dataRow2 = new DataRow();
    private DataRow dataRow3 = new DataRow();
    private DataRow dataRow4 = new DataRow();

    @BeforeMethod public void setUp() throws Exception {
        columnAggrTypes.add(AggregateFunc.COUNT);
        columnAggrTypes.add(AggregateFunc.SUM);
        columnAggrTypes.add(AggregateFunc.MAX);
        columnAggrTypes.add(AggregateFunc.MIN);
        columnAggrTypes.add(AggregateFunc.MIN);

        RowDescription.PGColumn[] columns =
            {new RowDescription.PGColumn(), new RowDescription.PGColumn(),
                new RowDescription.PGColumn(), new RowDescription.PGColumn(),
                new RowDescription.PGColumn()};
        for (int i = 0; i < columns.length; i++) {
            columns[i].setName(columnAggrTypes.get(i).name());
            columns[i].setTableOid(16419);
            columns[i].setColNo(i + 1);
            columns[i].setTypeOid(20);
            columns[i].setTypeLen(8);
            columns[i].setTypeMod(-1);
            columns[i].setFormat(0);
        }

        rowDescription.setColumns(columns);

        DataRow.PGCol[] cols1 = new DataRow.PGCol[columnAggrTypes.size()];
        cols1[0] = new DataRow.PGCol("1".getBytes(StandardCharsets.UTF_8));
        cols1[1] = new DataRow.PGCol("1".getBytes(StandardCharsets.UTF_8));
        cols1[2] = new DataRow.PGCol("1".getBytes(StandardCharsets.UTF_8));
        cols1[3] = new DataRow.PGCol("1".getBytes(StandardCharsets.UTF_8));
        cols1[4] = new DataRow.PGCol("1".getBytes(StandardCharsets.UTF_8));
        dataRow1.setCols(cols1);

        DataRow.PGCol[] cols2 = new DataRow.PGCol[columnAggrTypes.size()];
        cols2[0] = new DataRow.PGCol("2".getBytes(StandardCharsets.UTF_8));
        cols2[1] = new DataRow.PGCol("2.01".getBytes(StandardCharsets.UTF_8));
        cols2[2] = new DataRow.PGCol("12345678901234567890".getBytes(StandardCharsets.UTF_8));
        cols2[3] = new DataRow.PGCol("-2".getBytes(StandardCharsets.UTF_8));
        cols2[4] = new DataRow.PGCol(null);
        dataRow2.setCols(cols2);

        DataRow.PGCol[] cols3 = new DataRow.PGCol[columnAggrTypes.size()];
        cols3[0] = new DataRow.PGCol("3".getBytes(StandardCharsets.UTF_8));
        cols3[1] = new DataRow.PGCol(null);
        cols3[2] = new DataRow.PGCol(null);
        cols3[3] = new DataRow.PGCol(null);
        cols3[4] = new DataRow.PGCol(null);
        dataRow3.setCols(cols3);

        DataRow.PGCol[] cols4 = new DataRow.PGCol[columnAggrTypes.size()];
        cols4[0] = new DataRow.PGCol("0".getBytes(StandardCharsets.UTF_8));
        cols4[1] = new DataRow.PGCol("0".getBytes(StandardCharsets.UTF_8));
        cols4[2] = new DataRow.PGCol("INF".getBytes(StandardCharsets.UTF_8));
        cols4[3] = new DataRow.PGCol("abc".getBytes(StandardCharsets.UTF_8));
        cols4[4] = new DataRow.PGCol("NaN".getBytes(StandardCharsets.UTF_8));
        dataRow4.setCols(cols4);
    }

    @Test public void testToPartPackets() throws Exception {
        ShardingResultSet result = new PGShardingResultSet(columnAggrTypes, false);
        result.addColumn(Collections.singletonList(rowDescription.toPacket()), 1);

        result.addRow(Collections.singletonList(dataRow1.toPacket()), 1);
        result.addRow(Collections.singletonList(dataRow2.toPacket()), 2);
        result.addRow(Collections.singletonList(dataRow3.toPacket()), 3);

        List<byte[]> aggregrateBytes = result.toPartPackets(false);

        DataRow dataRow = DataRow.loadFromPacket(aggregrateBytes.get(1));
        DataRow.PGCol[] cols = dataRow.getCols();
        Assert.assertEquals(cols[0].getData(), "6".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(cols[1].getData(), "3.01".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(cols[2].getData(),
            "12345678901234567890".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(cols[3].getData(), "-2".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(cols[4].getData(), "1".getBytes(StandardCharsets.UTF_8));
    }

    @Test public void testAggregrateWithGroupBy() throws Exception {
        ShardingResultSet result = new PGShardingResultSet(columnAggrTypes, true);

        result.addColumn(Collections.singletonList(rowDescription.toPacket()), 1);
        List<byte[]> row1s = new ArrayList<>();
        row1s.add(dataRow1.toPacket());
        row1s.add(dataRow2.toPacket());
        result.addRow(row1s, 1);
        result.addCommandComplete(new CommandComplete("SELECT 2").toPacket());
        result.addReadyForQuery(ReadyForQuery.IDLE.toPacket());

        result.addColumn(Collections.singletonList(rowDescription.toPacket()), 2);
        List<byte[]> row2s = new ArrayList<>();
        row2s.add(dataRow3.toPacket());
        row2s.add(dataRow4.toPacket());
        result.addRow(row2s, 2);
        result.addCommandComplete(new CommandComplete("SELECT 2").toPacket());
        result.addReadyForQuery(ReadyForQuery.IDLE.toPacket());

        List<byte[]> aggregrateBytes = result.toPartPackets(true);

        Assert.assertEquals(aggregrateBytes.size(), 7);

    }

    @Test public void testToCompletePartPackets() throws Exception {
        ShardingResultSet result = new PGShardingResultSet(columnAggrTypes, false);
        result.addColumn(Collections.singletonList(rowDescription.toPacket()), 1);

        result.addRow(Collections.singletonList(dataRow1.toPacket()), 1);
        result.addCommandComplete(new CommandComplete("SELECT 1").toPacket());
        result.addReadyForQuery(ReadyForQuery.IDLE.toPacket());

        result.addRow(Collections.singletonList(dataRow2.toPacket()), 2);
        result.addCommandComplete(new CommandComplete("SELECT 1").toPacket());
        result.addReadyForQuery(ReadyForQuery.IDLE.toPacket());

        List<byte[]> aggregrateBytes = result.toPartPackets(true);

        Assert.assertEquals(aggregrateBytes.size(), 4);

        byte[] commandCompleteBytes = aggregrateBytes.get(2);

        Assert.assertEquals(PGMessage.getType(commandCompleteBytes), PGFlags.COMMAND_COMPLETE);

        Assert.assertEquals(1, CommandComplete.loadFromPacket(commandCompleteBytes).getRows());
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "inconsistent readyForQuery Status .* when validate PostgreSQL shard response")
    public void testFailCompletePartPackets() throws Exception {
        ShardingResultSet result = new PGShardingResultSet(columnAggrTypes, false);
        result.addColumn(Collections.singletonList(rowDescription.toPacket()), 1);

        result.addRow(Collections.singletonList(dataRow1.toPacket()), 1);
        result.addCommandComplete(new CommandComplete("SELECT 1").toPacket());
        result.addReadyForQuery(ReadyForQuery.IDLE.toPacket());

        result.addRow(Collections.singletonList(dataRow2.toPacket()), 2);
        result.addCommandComplete(new CommandComplete("SELECT 1").toPacket());
        result.addReadyForQuery(ReadyForQuery.IN_TX.toPacket());

        result.toPartPackets(true);
    }

    @Test public void testNaNToPartPackets() throws Exception {
        ShardingResultSet result = new PGShardingResultSet(columnAggrTypes, false);
        result.addColumn(Collections.singletonList(rowDescription.toPacket()), 1);

        result.addRow(Collections.singletonList(dataRow1.toPacket()), 1);
        result.addRow(Collections.singletonList(dataRow2.toPacket()), 2);
        result.addRow(Collections.singletonList(dataRow3.toPacket()), 3);
        result.addRow(Collections.singletonList(dataRow4.toPacket()), 4);

        List<byte[]> aggregrateBytes = result.toPartPackets(false);

        DataRow dataRow = DataRow.loadFromPacket(aggregrateBytes.get(1));
        DataRow.PGCol[] cols = dataRow.getCols();
        Assert.assertEquals(cols[0].getData(), "6".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(cols[1].getData(), "3.01".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(cols[2].getData(),
            "12345678901234567890".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(cols[3].getData(), "-2".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(cols[4].getData(), "1".getBytes(StandardCharsets.UTF_8));
    }

    @AfterMethod public void tearDown() throws Exception {
        columnAggrTypes.clear();
    }
}
