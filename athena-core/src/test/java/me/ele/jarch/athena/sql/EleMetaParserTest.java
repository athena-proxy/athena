package me.ele.jarch.athena.sql;

import org.testng.annotations.Test;

import java.util.Map;

import static org.testng.Assert.*;

/**
 * Created by Dal-Dev-Team on 16/12/28.
 */
public class EleMetaParserTest {

    @Test public void testParseRawSQL() {
        String sql =
            "/* E:role=slave&shardkey=shardid&appid=zeus.eos&shardvalue=8&rid=nevermore.api^^6B629D7FC725477AB5CD644B91F5E2FB|1528967046307&rpcid=1.1.4.1.2.1:E */SELECT 1";
        EleMetaParser parser = EleMetaParser.parse(sql);
        assertEquals(parser.getQueryComment(),
            "/* E:role=slave&shardkey=shardid&appid=zeus.eos&shardvalue=8&rid=nevermore.api^^6B629D7FC725477AB5CD644B91F5E2FB|1528967046307&rpcid=1.1.4.1.2.1:E */");
        assertEquals(parser.getQueryWithoutComment(), "SELECT 1");
        assertEquals(parser.getEleMeta().get("role"), "slave");
        assertEquals(parser.getEleMeta().get("shardkey"), "shardid");
        assertEquals(parser.getEleMeta().get("appid"), "zeus.eos");
        assertEquals(parser.getEleMeta().get("shardvalue"), "8");
        assertEquals(parser.getEleMeta().get("rid"),
            "nevermore.api^^6B629D7FC725477AB5CD644B91F5E2FB|1528967046307");
        assertEquals(parser.getEleMeta().get("rpcid"), "1.1.4.1.2.1");
    }

    @Test public void testParsePython() {
        String sql = "/*E:tranid=1&bind_db=master&priority=123 :E*/SELECT 1";
        EleMetaParser parser = EleMetaParser.parse(sql);
        Map<String, String> rel = parser.getEleMeta();
        assertEquals("1", rel.get("tranid"));
        assertEquals("master", rel.get("bind_db"));
        assertEquals("123", rel.get("priority"));
    }

    @Test public void testParseHibernate() {
        String sql = "/* E:tranid=1&bind_db=master&priority=123 :E */ SELECT 1";
        EleMetaParser parser = EleMetaParser.parse(sql);
        Map<String, String> rel = parser.getEleMeta();
        assertEquals("1", rel.get("tranid"));
        assertEquals("master", rel.get("bind_db"));
        assertEquals("123", rel.get("priority"));
    }

    @Test public void testParsePropertilesWithoutAmpersand() {
        String sql = "/*E: bind_db=master priority=123:E  */SELECT 1";
        EleMetaParser parser = EleMetaParser.parse(sql);
        Map<String, String> rel = parser.getEleMeta();
        assertEquals(rel.size(), 1);
        assertEquals("master priority=123", rel.get("bind_db"));
        assertNotEquals("master", rel.get("bind_db"),
            "only & is used to separate the key-value pairs");
    }

    @Test public void testParsePropertilesWithWrongTag() {
        String sql = "/*bind_db=master*//*E: a=b :E*/SELECT 1";
        EleMetaParser parser = EleMetaParser.parse(sql);
        Map<String, String> rel = parser.getEleMeta();
        assertEquals(rel.size(), 0);
    }

    @Test public void valueHasClosedEleComment() {
        String sql = "/*bind_db=master*/select a from b where c = '/*E:a = b:E*/'";
        EleMetaParser parser = EleMetaParser.parse(sql);
        Map<String, String> rel = parser.getEleMeta();
        assertEquals(rel.size(), 0);
        assertEquals(parser.getQueryComment(), "");
        assertEquals(parser.getQueryWithoutComment(),
            "/*bind_db=master*/select a from b where c = '/*E:a = b:E*/'");
    }

    @Test public void onlyValueHasClosedEleComment() {
        String sql = "select a from b where c = '/*E:a = b:E*/'";
        EleMetaParser parser = EleMetaParser.parse(sql);
        Map<String, String> rel = parser.getEleMeta();
        assertEquals(rel.size(), 0);
        assertEquals(parser.getQueryComment(), "");
        assertEquals(parser.getQueryWithoutComment(), "select a from b where c = '/*E:a = b:E*/'");
    }

    @Test public void testParsePropertilesWithWhiteSpaces() {
        String sql = "/*E:bind_db = master :E*/SELECT 1";
        EleMetaParser parser = EleMetaParser.parse(sql);
        Map<String, String> rel = parser.getEleMeta();
        assertEquals("master", rel.get("bind_db"), "White spaces should be ignored");
    }

    @Test public void testParsePropertilesWithDoubleEqualSign() {
        String sql = "/*E:bind_db==master:E*/";
        EleMetaParser parser = EleMetaParser.parse(sql);
        Map<String, String> rel = parser.getEleMeta();
        assertEquals("=master", rel.get("bind_db"),
            "First = is used to separate the key and value");
    }

    @Test public void testParsePropertilesWithEmptyValue() {
        String sql = "/*E:bind_db=:E*/";
        EleMetaParser parser = EleMetaParser.parse(sql);
        Map<String, String> rel = parser.getEleMeta();
        assertEquals("", rel.get("bind_db"), "A propertie can have an empty value");
    }

    @Test public void testParsePropertilesWithEmptyKey() {
        String sql = "/*E:=master:E*/";
        EleMetaParser parser = EleMetaParser.parse(sql);
        Map<String, String> rel = parser.getEleMeta();
        assertEquals(rel.get(""), "master", "A property must have a nonempty key");
    }

    @Test public void testNormalComment() {
        String sql = "/*abc*/SELECT 1";
        EleMetaParser parser = EleMetaParser.parse(sql);
        Map<String, String> rel = parser.getEleMeta();
        assertTrue(rel.isEmpty());
    }

    @Test public void testOnlyEleComment() {
        String sql = "/*E::E*/";
        EleMetaParser parser = EleMetaParser.parse(sql);
        Map<String, String> rel = parser.getEleMeta();
        assertTrue(rel.isEmpty());
    }

    @Test public void nestedEleComment() {
        String sql = "/*E:/*E::E*/:E*/SELECT 1";
        EleMetaParser parser = EleMetaParser.parse(sql);
        Map<String, String> rel = parser.getEleMeta();
        assertEquals(parser.getQueryComment(), "/*E:/*E::E*/");
        assertEquals(parser.getQueryWithoutComment(), ":E*/SELECT 1");
        assertTrue(rel.isEmpty());
    }

    @Test public void valueHasEleComment() {
        String sql = "/*E:a = b:E*/SELECT a from b where c = ':E*/text'";
        EleMetaParser parser = EleMetaParser.parse(sql);
        Map<String, String> rel = parser.getEleMeta();
        assertEquals(parser.getQueryComment(), "/*E:a = b:E*/");
        assertEquals(parser.getQueryWithoutComment(), "SELECT a from b where c = ':E*/text'");
        assertEquals(rel.get("a"), "b");
    }

}
