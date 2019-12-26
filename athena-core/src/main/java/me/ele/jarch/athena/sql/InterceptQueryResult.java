package me.ele.jarch.athena.sql;

import com.github.mpjct.jmpjct.mysql.proto.MysqlResponseClassifier;
import me.ele.jarch.athena.util.proto.ResponseClassifier;

/**
 * Created by jinghao.wang on 16/8/4.
 */
public class InterceptQueryResult extends QueryResultContext {
    public final ResponseClassifier responseClassifier;

    public InterceptQueryResult() {
        responseClassifier = newResponseClassifier();
    }

    protected ResponseClassifier newResponseClassifier() {
        return new MysqlResponseClassifier();
    }

    @Override public void writeBytes(byte[] src) {
        sqlCtx.currentWriteToDalCounts += src.length;
    }

    @Override public void addColumn(byte[] column) {
        super.addColumn(column);
        responseClassifier.addColumn(column);
    }

    @Override public void addRow(byte[] row) {
        super.addRow(row);
        responseClassifier.addRow(row);
    }

    @Override public void sendAndFlush2Client() {
        throw new RuntimeException("Called sendAndFlush2Client in InterceptQueryResultContext");
    }
}
