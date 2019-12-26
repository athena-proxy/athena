package com.github.mpjct.jmpjct.mysql.proto;

import me.ele.jarch.athena.sql.EleMetaParser;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class Com_QueryTests {

    @Test public void test_handleELEMetaFromQUeryPython() {
        Com_Query comQuery = new Com_Query();
        comQuery.setQuery("/*E:bind-db=master:E*/select * from user");
        Com_Query newcomQuery = Com_Query.loadFromPacket(comQuery.toPacket());
        EleMetaParser parser = EleMetaParser.parse(newcomQuery.getQuery());
        assertEquals(parser.getEleMeta().get("bind-db"), "master");
        // assertEquals(newcomQuery.query, "select * from user");
    }

    @Test public void test_handleELEMetaFromQUeryHibernate() {
        Com_Query comQuery = new Com_Query();
        comQuery.setQuery("/* E:bind-db=master:E */select * from user");
        Com_Query newcomQuery = Com_Query.loadFromPacket(comQuery.toPacket());
        EleMetaParser parser = EleMetaParser.parse(newcomQuery.getQuery());
        assertEquals(parser.getEleMeta().get("bind-db"), "master");
        // assertEquals(newcomQuery.query, "select * from user");
    }

    @Test public void test_handleELEMetaFromQUeryWithoutELeMate() {
        Com_Query comQuery = new Com_Query();
        comQuery.setQuery("/*some comments*/select * from user");
        Com_Query newcomQuery = Com_Query.loadFromPacket(comQuery.toPacket());
        EleMetaParser parser = EleMetaParser.parse(newcomQuery.getQuery());
        assertTrue(parser.getEleMeta().isEmpty());
        // assertEquals(newcomQuery.query, "/*some comments*/select * from user");
    }

    @Test public void testCommentEndSymbolInSql() {
        Com_Query comQuery = new Com_Query();
        comQuery.setQuery("/* E:shardid=32:E */insert into atest (id, name) values (123, :E */)");
        Com_Query newcomQuery = Com_Query.loadFromPacket(comQuery.toPacket());
        EleMetaParser parser = EleMetaParser.parse(newcomQuery.getQuery());
        assertEquals(parser.getQueryWithoutComment(),
            "insert into atest (id, name) values (123, :E */)");
    }
}
