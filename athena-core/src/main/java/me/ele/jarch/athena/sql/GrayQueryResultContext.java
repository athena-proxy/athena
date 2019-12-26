package me.ele.jarch.athena.sql;

import me.ele.jarch.athena.exception.QueryException;

public class GrayQueryResultContext extends QueryResultContext {


    public GrayQueryResultContext() {
    }

    private boolean endTrans = false;

    public GrayQueryResultContext(boolean endTrans) {
        this.endTrans = endTrans;
    }

    public boolean isEndTrans() {
        return endTrans;
    }

    public void writeBytes(byte[] src) {
    }

    public void addColumn(byte[] column) {
    }

    public void SetRequltType(ResultType type) {
        this.resultType = type;
    }

    public void addRow(byte[] row) {
    }

    public void sendAndFlush2Client() {
        throw new RuntimeException("Cannot call sendAndFlush2Client.");
    }

    public void assertMaxResultSize() throws QueryException {
    }

}
