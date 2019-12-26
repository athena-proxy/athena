package me.ele.jarch.athena.sql.seqs;

import com.github.mpjct.jmpjct.mysql.proto.Column;
import com.github.mpjct.jmpjct.mysql.proto.ERR;
import com.github.mpjct.jmpjct.mysql.proto.ResultSet;
import com.github.mpjct.jmpjct.mysql.proto.Row;

import java.util.ArrayList;
import java.util.List;

public class GeneratorUtil {
    public static final String FIRST_DIM = "first_dim";
    public static final String SECOND_DIM = "second_dim";

    public static final String PREFIX = "prefix";

    public static final String SHARDING_COUNT = "sharding_count";
    public static final String NEXT_VALUE = "next_value";
    public static final String NEXT_BEGIN = "next_begin";
    public static final String NEXT_END = "next_end";
    public static final String DAL_DUAL = "DAL_DUAL";
    public static final String SEQ_NAME = "seq_name";

    public static final String COMMON_SEQ = "common_seq";
    public static final String COMPOSED_SEQ = "composed_seq";
    public static final String FIX_HASH_SEQ = "fix_hash_seq";

    public static final String BIZ = "biz";
    public static final String COUNT = "count";

    public static ArrayList<byte[]> getIDPackets(String columnName, List<String> globalIds) {

        ResultSet rs = new ResultSet();
        rs.addColumn(buildColumn(columnName));

        for (String globalId : globalIds) {
            rs.addRow(new Row(globalId));
        }
        return rs.toPackets();
    }

    public static ArrayList<byte[]> getSerialIDPackets(String start, String end,
        List<String> globalIds) {

        ResultSet rs = new ResultSet();
        rs.addColumn(buildColumn(start));
        rs.addColumn(buildColumn(end));

        rs.addRow(new Row(globalIds.get(0), globalIds.get(1)));
        return rs.toPackets();
    }

    public static byte[] getErrPacket(String columnName) {
        String errorMessage = String.format("Unknown column '%s' in 'field list'", columnName);
        ERR err = ERR.buildErr(1, 1054, "42S22", errorMessage);
        return err.toPacket();
    }

    private static Column buildColumn(String value) {
        Column col = new Column(value);
        col.table = "dal_dual";
        col.org_table = "dal_dual";
        col.org_name = value;
        col.characterSet = (byte) (33 & 0xff);// utf8
        return col;
    }
}
