package me.ele.jarch.athena.sql;

import me.ele.jarch.athena.pg.proto.DataRow;
import me.ele.jarch.athena.pg.proto.DataRow.PGCol;
import me.ele.jarch.athena.pg.proto.PGResponseClassifier;
import me.ele.jarch.athena.pg.proto.RowDescription;
import me.ele.jarch.athena.pg.proto.RowDescription.PGColumn;
import me.ele.jarch.athena.util.proto.ResponseClassifier;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class PGResultSet extends ResultSet {
    private PGResultSet(ResponseClassifier drs, ResultType resultType) {
        super(drs, resultType);
    }

    @Override protected Object getData(int columnIndex) {
        checkParam(columnIndex);
        DataRow dataRow = DataRow.loadFromPacket(drs.rows.get(rowIndex));
        PGCol pgcol = dataRow.getCols()[columnIndex - 1];
        return new String(pgcol.getData(), StandardCharsets.UTF_8);
    }

    @Override public String toString() {
        return String
            .format("PGResultSet [rowCount=%d,colCount=%d, resultType=%s,err=%s]", drs.rows.size(),
                drs.columns.size(), resultType, err);
    }

    public static PGResultSet newPGResultSet(RowDescription rowDesc, List<DataRow> dataRows,
        ResultType resultType) {
        ResponseClassifier drs = new PGResponseClassifier();
        PGColumn[] columns = rowDesc.getColumns();
        if (columns != null) {
            for (PGColumn column : columns) {
                drs.addColumn(column.toPacket());
            }
        }
        for (DataRow dataRow : dataRows) {
            drs.addRow(dataRow.toPacket());
        }
        return new PGResultSet(drs, resultType);
    }
}
