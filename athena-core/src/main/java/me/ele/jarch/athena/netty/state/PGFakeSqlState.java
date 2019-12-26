package me.ele.jarch.athena.netty.state;

import io.netty.buffer.Unpooled;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.pg.proto.DataRow;
import me.ele.jarch.athena.pg.proto.ReadyForQuery;
import me.ele.jarch.athena.pg.proto.ResultSet;
import me.ele.jarch.athena.pg.proto.RowDescription;

import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by jinghao.wang on 2017/12/14.
 */
public class PGFakeSqlState extends FakeSqlState {
    public PGFakeSqlState(SqlSessionContext sqlSessionContext) {
        super(sqlSessionContext);
    }

    @Override protected void write2Client(String title, List<String> msgs) {
        final ResultSet resultSet = new ResultSet();
        RowDescription.PGColumn[] columns = {new RowDescription.PGColumn()};
        RowDescription.PGColumn column = columns[0];
        column.setName(title);
        column.setColNo(1);
        column.setTypeLen(-1);
        column.setTypeMod(-1);
        column.setFormat(0);
        resultSet.setRowDescription(new RowDescription(columns));
        msgs.forEach(msg -> {
            DataRow.PGCol[] cols = {new DataRow.PGCol(msg.getBytes(StandardCharsets.UTF_8))};
            DataRow dataRow = new DataRow(cols);
            resultSet.addDataRow(dataRow);
        });
        resultSet.setReadyForQuery(
            sqlSessionContext.isInTransStatus() ? ReadyForQuery.IN_TX : ReadyForQuery.IDLE);
        sqlSessionContext.clientWriteAndFlush(
            Unpooled.wrappedBuffer(resultSet.toPackets().toArray(new byte[0][])));
    }

    @Override protected List<String> doDynamicHelps() {
        List<String> dynamicHelps = new LinkedList<>();
        dynamicHelps
            .add("------------------------------------------------------------------------");
        dynamicHelps.add("The following help SQLs suit for current protocol");
        dynamicHelps.add(String.format("SELECT %s FROM dal_dual where \"group\" = '%s'", DB_CONFIG,
            sqlSessionContext.getHolder().getDalGroup().getName()));
        dynamicHelps.add(
            String.format("SELECT %s FROM dal_dual WHERE \"group\" = 'eos_group'", KILL_GROUP));
        return dynamicHelps;
    }
}
