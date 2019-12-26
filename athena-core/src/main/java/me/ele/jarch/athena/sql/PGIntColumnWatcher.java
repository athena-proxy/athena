package me.ele.jarch.athena.sql;

import me.ele.jarch.athena.netty.PGWatchableColumn;
import me.ele.jarch.athena.netty.WatchableColumn;
import me.ele.jarch.athena.pg.proto.DataRow;
import me.ele.jarch.athena.pg.proto.RowDescription;
import me.ele.jarch.athena.pg.util.PGAuthenticator;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

/**
 * Created by zhengchao on 2017/2/27.
 */
public class PGIntColumnWatcher extends AbstractIntColumnWatcher {

    @Override protected List<String> extractContent(final byte[] packet) {
        PGAuthenticator auth = (PGAuthenticator) (sqlCtx.authenticator);
        String clientEncoding = auth.getClientAttributes().getOrDefault("client_encoding", null);
        if (!Objects.isNull(clientEncoding) && !clientEncoding.toUpperCase().equals("UTF8")) {
            return new ArrayList<>(0);
        }

        List<String> columns = new LinkedList<>();
        DataRow dataRow = DataRow.loadFromPacket(packet);
        for (DataRow.PGCol col : dataRow.getCols()) {
            if (Objects.isNull(col.getData())) {
                continue;
            }
            columns.add(new String(col.getData(), StandardCharsets.UTF_8));
        }

        return columns;
    }

    @Override public void addWatchingColumnIfNecessary(final byte[] cmdColPacket) {
        RowDescription rowDescription = RowDescription.loadFromPacket(cmdColPacket);
        if (rowDescription.getColCount() <= 0) {
            return;
        }

        int columnIndex = 0;
        for (RowDescription.PGColumn column : rowDescription.getColumns()) {
            WatchableColumn watchableColumn = PGWatchableColumn.createWatchableColumn(column);
            if (!Objects.isNull(watchableColumn)) {
                watchingColumns.put(columnIndex, watchableColumn);
            }
            ++columnIndex;
        }
    }
}
