package me.ele.jarch.athena.sql;

import com.github.mpjct.jmpjct.mysql.proto.Column;
import com.github.mpjct.jmpjct.mysql.proto.Row;
import me.ele.jarch.athena.netty.MysqlWatchableColumn;
import me.ele.jarch.athena.netty.WatchableColumn;

import java.util.List;
import java.util.Objects;

/**
 * Created by zhengchao on 2017/2/27.
 */
public class MysqlIntColumnWatcher extends AbstractIntColumnWatcher {

    @Override protected List<String> extractContent(final byte[] packet) {
        return Row.loadColumnsFromPacket(packet);
    }


    @Override protected void addWatchingColumnIfNecessary(final byte[] cmdColPacket) {
        Column column = Column.loadFromPacket(cmdColPacket);
        WatchableColumn watchableColumn = MysqlWatchableColumn.createWatchableColumn(column);
        if (!Objects.isNull(watchableColumn)) {
            watchingColumns.put(columnIndexForWatching, watchableColumn);
        }
        ++columnIndexForWatching;
    }
}
