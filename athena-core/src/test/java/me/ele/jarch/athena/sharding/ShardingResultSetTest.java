package me.ele.jarch.athena.sharding;

import com.github.mpjct.jmpjct.mysql.proto.Column;
import com.github.mpjct.jmpjct.mysql.proto.Flags;
import com.github.mpjct.jmpjct.mysql.proto.Row;
import me.ele.jarch.athena.util.AggregateFunc;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class ShardingResultSetTest {

    private List<byte[]> columns = new ArrayList<>();

    @BeforeClass public void setUp() {
        Column c1 = new Column("");
        c1.type = Flags.MYSQL_TYPE_DATE;
        Column c2 = new Column("");
        c2.type = Flags.MYSQL_TYPE_TIME;
        Column c3 = new Column("");
        c3.type = Flags.MYSQL_TYPE_YEAR;
        Column c4 = new Column("");
        c4.type = Flags.MYSQL_TYPE_TIMESTAMP;
        Column c5 = new Column("");
        c5.type = Flags.MYSQL_TYPE_TIMESTAMP;
        columns.add(c1.toPacket());
        columns.add(c2.toPacket());
        columns.add(c3.toPacket());
        columns.add(c4.toPacket());
        columns.add(c5.toPacket());
    }

    @Test public void aggregateRows() {
        List<AggregateFunc> columnAggeTypes = new ArrayList<>();
        columnAggeTypes.add(AggregateFunc.COUNT);
        columnAggeTypes.add(AggregateFunc.SUM);
        columnAggeTypes.add(AggregateFunc.MAX);
        columnAggeTypes.add(AggregateFunc.MIN);
        columnAggeTypes.add(AggregateFunc.MIN);
        ShardingResultSet result = new MysqlShardingResultSet(columnAggeTypes, false);
        List<byte[]> columns = new ArrayList<>();
        columnAggeTypes.forEach(type -> {
            columns.add(new Column(type.name()).toPacket());
        });
        result.addColumn(columns, 1);
        Row r1 = new Row();
        r1.addData("1");
        r1.addData("1");
        r1.addData("1");
        r1.addData("1");
        r1.addData((String) null);
        List<byte[]> row1s = new ArrayList<>();
        row1s.add(r1.toPacket());
        result.addRow(row1s, 1);

        Row r2 = new Row();
        r2.addData("2");
        r2.addData("2.01");
        r2.addData("12345678901234567890");
        r2.addData("-2");
        r2.addData((String) null);
        List<byte[]> row2s = new ArrayList<>();
        row2s.add(r2.toPacket());
        result.addRow(row2s, 2);

        Row r3 = new Row();
        r3.addData("3");
        r3.addData((String) null);
        r3.addData((String) null);
        r3.addData((String) null);
        r3.addData((String) null);
        List<byte[]> row3s = new ArrayList<>();
        row3s.add(r3.toPacket());
        result.addRow(row3s, 3);

        Row aaggregatedRow = Row.loadFromPacket(result.toPartPackets(false).get(7), 5);
        Assert.assertEquals(aaggregatedRow.toString(), "6,3.01,12345678901234567890,-2,null");
    }

    @Test public void aggregateDateTimeRows() {
        List<AggregateFunc> columnAggeTypes = new ArrayList<>();
        columnAggeTypes.add(AggregateFunc.MIN);
        columnAggeTypes.add(AggregateFunc.MAX);
        columnAggeTypes.add(AggregateFunc.MAX);
        columnAggeTypes.add(AggregateFunc.MIN);
        columnAggeTypes.add(AggregateFunc.MIN);
        ShardingResultSet result = new MysqlShardingResultSet(columnAggeTypes, false);

        result.addColumn(columns, 1);
        Row r1 = new Row();
        r1.addData("2017-08-12");
        r1.addData("11:11:00");
        r1.addData("2017");
        r1.addData("2017-08-12 11:11:00");
        r1.addData("2017-08-12 11:11:00.10000");
        List<byte[]> row1s = new ArrayList<>();
        row1s.add(r1.toPacket());
        result.addRow(row1s, 1);
        result.addRow(row1s, 2);

        Row r2 = new Row();
        r2.addData("2017-08-11");
        r2.addData("11:11:01");
        r2.addData("2018");
        r2.addData("2017-08-12 11:11:01");
        r2.addData("2017-08-12 11:11:00.10001");
        List<byte[]> row2s = new ArrayList<>();
        row2s.add(r2.toPacket());
        result.addRow(row2s, 3);
        Row aggregatedRow = Row.loadFromPacket(result.toPartPackets(false).get(7), 5);
        Assert.assertEquals(aggregatedRow.toString(),
            "2017-08-11,11:11:01,2018,2017-08-12 11:11:00,2017-08-12 11:11:00.10000");
    }

    public void benchmark() {
        String value = "11";
        long start = System.currentTimeMillis();
        long re = 0;
        for (int i = 1; i < 10000000; i++) {
            long v = Long.parseLong(value);
            re = re > v ? re : v;
            value = value == "11" ? "12" : "11";
        }
        System.out.println(System.currentTimeMillis() - start);

        start = System.currentTimeMillis();
        BigDecimal redb = new BigDecimal(0);
        for (int i = 1; i < 10000000; i++) {
            BigDecimal v = new BigDecimal(value);
            redb = redb.compareTo(v) > 0 ? redb : v;
            value = value == "11" ? "12" : "11";
        }
        System.out.println(System.currentTimeMillis() - start);
    }
}
