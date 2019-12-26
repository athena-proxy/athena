package me.ele.jarch.athena.pg.proto;

import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static org.testng.Assert.assertEquals;

/**
 * Created by jinghao.wang on 2017/12/14.
 */
public class ResultSetTest {

    @Test public void legalResultSet() throws Exception {
        final ResultSet resultSet = new ResultSet();
        RowDescription.PGColumn[] columns = {new RowDescription.PGColumn()};
        RowDescription.PGColumn column = columns[0];
        column.setName("test");
        column.setColNo(1);
        column.setTypeLen(-1);
        column.setTypeMod(-1);
        column.setFormat(0);
        resultSet.setRowDescription(new RowDescription(columns));
        List<String> msgs = Arrays.asList("1", "2", "3");
        msgs.forEach(msg -> {
            DataRow.PGCol[] cols = {new DataRow.PGCol(msg.getBytes(StandardCharsets.UTF_8))};
            DataRow dataRow = new DataRow(cols);
            resultSet.addDataRow(dataRow);
        });
        resultSet.setReadyForQuery(ReadyForQuery.IDLE);

        List<byte[]> packets = resultSet.toPackets();
        CommandComplete commandComplete = CommandComplete.loadFromPacket(packets.get(4));
        assertEquals(commandComplete.getRows(), 3);
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "no RowDescription")
    public void missRowDescription() throws Exception {
        final ResultSet resultSet = new ResultSet();
        List<String> msgs = Arrays.asList("1", "2", "3");
        msgs.forEach(msg -> {
            DataRow.PGCol[] cols = {new DataRow.PGCol(msg.getBytes(StandardCharsets.UTF_8))};
            DataRow dataRow = new DataRow(cols);
            resultSet.addDataRow(dataRow);
        });
        resultSet.setReadyForQuery(ReadyForQuery.IDLE);
        resultSet.toPackets();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "no ReadyForQuery")
    public void missReadyForQuery() throws Exception {
        final ResultSet resultSet = new ResultSet();
        RowDescription.PGColumn[] columns = {new RowDescription.PGColumn()};
        RowDescription.PGColumn column = columns[0];
        column.setName("test");
        column.setColNo(1);
        column.setTypeLen(-1);
        column.setTypeMod(-1);
        column.setFormat(0);
        resultSet.setRowDescription(new RowDescription(columns));
        List<String> msgs = Arrays.asList("1", "2", "3");
        msgs.forEach(msg -> {
            DataRow.PGCol[] cols = {new DataRow.PGCol(msg.getBytes(StandardCharsets.UTF_8))};
            DataRow dataRow = new DataRow(cols);
            resultSet.addDataRow(dataRow);
        });
        resultSet.toPackets();
    }
}
