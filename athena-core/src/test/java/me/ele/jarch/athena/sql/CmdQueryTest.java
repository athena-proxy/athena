package me.ele.jarch.athena.sql;

import com.github.mpjct.jmpjct.mysql.proto.Com_Ping;
import com.github.mpjct.jmpjct.mysql.proto.Com_Query;
import com.github.mpjct.jmpjct.mysql.proto.Flags;
import me.ele.jarch.athena.pg.proto.PGFlags;
import me.ele.jarch.athena.pg.proto.Query;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * Created by Dal-Dev-Team on 17/3/18.
 */
public class CmdQueryTest {
    @Test public void testPing() throws Exception {
        CmdQuery query = new CmdQuery(new Com_Ping().toPacket(), "eosgroup");
        query.execute();
        assertEquals(query.shardingSql.getOriginMostSafeSQL(), CmdQuery.PING_SQL);
        assertEquals(query.type, Flags.COM_PING);
        assertEquals(query.queryType, QUERY_TYPE.MYSQL_PING);
    }

    @Test public void testParseFailShortSQLPattern() throws Exception {
        String invalidSQL = "slect * from eleme_order";
        Com_Query comQuery = new Com_Query();
        comQuery.setQuery(invalidSQL);
        CmdQuery query = new CmdQuery(comQuery.toPacket(), "eosgroup");
        query.execute();
        assertEquals("[dal_parse_fail] slect * from eleme_order", query.getSqlPattern());
    }

    @Test public void testParseFailLongSQLPattern() throws Exception {
        String invalidSQL = "slect * from eleme_order where 'longlonglonglong' = 'longlonglong'";
        Com_Query comQuery = new Com_Query();
        comQuery.setQuery(invalidSQL);
        CmdQuery query = new CmdQuery(comQuery.toPacket(), "eosgroup");
        query.execute();
        assertEquals("[dal_parse_fail] slect * from eleme_order where '", query.getSqlPattern());
    }

    @Test public void testPGEmptyQuery() throws Exception {
        CmdQuery query = new PGCmdQuery(new Query().toPacket(), "eosgroup");
        query.execute();
        assertEquals(query.shardingSql.getOriginMostSafeSQL(), CmdQuery.EMPTY_SQL);
        assertEquals(query.type, PGFlags.C_QUERY);
        assertEquals(query.queryType, QUERY_TYPE.PG_EMPTY_QUERY);
    }
}
