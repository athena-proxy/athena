package me.ele.jarch.athena.sql;

import com.github.mpjct.jmpjct.mysql.proto.ERR;
import com.github.mpjct.jmpjct.mysql.proto.OK;
import com.github.mpjct.jmpjct.mysql.proto.Row;
import me.ele.jarch.athena.util.proto.ResponseClassifier;

import java.util.List;
import java.util.Objects;

/**
 * Created by jinghao.wang on 16/8/8.
 */
public class ResultSet {
    protected final int rowCount;
    protected int rowIndex = -1;
    protected final ResponseClassifier drs;
    protected final ResultType resultType;
    protected final ERR err;
    protected final OK ok;

    public ResultSet(ResponseClassifier drs, ResultType resultType, ERR err, OK ok) {
        this.drs = drs;
        this.rowCount = drs.rows.size();
        this.resultType = resultType;
        this.err = err;
        this.ok = ok;
    }

    public ResultSet(ResponseClassifier drs, ResultType resultType) {
        this(drs, resultType, null, null);
    }

    public boolean next() {
        if (rowCount == 0) {
            return false;
        }
        if (rowIndex < rowCount - 1) {
            rowIndex++;
            return true;
        }
        return false;
    }

    @Override public String toString() {
        return String
            .format("ResultSet [rowCount=%d,colCount=%d]", drs.rows.size(), drs.columns.size());
    }

    protected void checkParam(int columnIndex) {
        if (columnIndex == 0) {
            throw new IndexOutOfBoundsException("ResultSet index start from 1,not 0 " + toString());
        }
        if (columnIndex > drs.columns.size()) {
            throw new IndexOutOfBoundsException("columnIndex out of bounds " + toString());
        }
        if (rowIndex < 0 || rowIndex >= rowCount) {
            throw new IndexOutOfBoundsException("index out of bounds: " + toString());
        }
    }

    /**
     * @throws IndexOutOfBoundsException will throw if columnIndex out of bounds
     */
    protected Object getData(int columnIndex) {
        checkParam(columnIndex);
        Row r = Row.loadFromPacket(drs.rows.get(rowIndex), drs.columns.size());
        return r.data.get(columnIndex - 1);
    }

    public ResultType getResultType() {
        return this.resultType;
    }

    public ERR getErr() {
        return this.err;
    }

    public boolean errorHappened() {
        return Objects.nonNull(err);
    }

    public boolean hasOKPacket() {
        return rowCount <= 0 && Objects.nonNull(ok);
    }

    public OK getOK() {
        return ok;
    }

    public int getRowCount() {
        return this.rowCount;
    }

    public List<byte[]> toPackets() {
        return this.drs.toPackets();
    }

    /**
     * Retrieves the value of the designated column in the current row
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @throws IndexOutOfBoundsException will throw if columnIndex out of bounds
     **/
    public String getString(int columnIndex) {
        Object o = getData(columnIndex);
        if (o instanceof String) {
            return (String) o;
        } else if (o == null) {
            return null;
        } else {
            throw new RuntimeException("This column type is not String " + o.getClass());
        }
    }
}
