package me.ele.jarch.athena.util;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Created by donxuu on 12/5/15.
 */
public class SQLRejecterTest {

    @Test public void testRejectSQL() throws Exception {
        SQLRejecter sqlRejecter = new SQLPatternRejecter("testGroup");
        sqlRejecter.setRejectPattern("Abc");
        boolean rejected = sqlRejecter.rejectSQL("originsql_not_used", "Abc", "127.0.0.0.1:12345");
        Assert.assertTrue(rejected);

        sqlRejecter.setRejectPattern("   ");
        rejected = sqlRejecter.rejectSQL("originsql_not_used", "Abc", "127.0.0.0.1:12345");
        Assert.assertFalse(rejected);

        final String originSQL =
            "select   id FROM tb_active_target WHERE target_type = 100 AND target_id=200";
        final String sqlPattern =
            "SELECT id FROM tb_active_target WHERE target_type = ? AND target_id = ?";
        sqlRejecter.setRejectPattern("4c15c759d8707a99b700666e51d0c3ef");
        rejected = sqlRejecter.rejectSQL(originSQL, sqlPattern, "127.0.0.0.1:12345");
        Assert.assertTrue(rejected);

        sqlRejecter.setRejectPattern("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        rejected = sqlRejecter.rejectSQL(originSQL, sqlPattern, "127.0.0.0.1:12345");
        Assert.assertFalse(rejected);

        sqlRejecter.setRejectPattern("4c15c759d8707a99b700666e51d0c3ef  target_id =   200");
        rejected = sqlRejecter.rejectSQL(originSQL, sqlPattern, "127.0.0.0.1:12345");
        Assert.assertTrue(rejected);

        sqlRejecter.setRejectPattern("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa  target_id =   200");
        rejected = sqlRejecter.rejectSQL(originSQL, sqlPattern, "127.0.0.0.1:12345");
        Assert.assertFalse(rejected);

        sqlRejecter.setRejectPattern("not_sqlid  target_id =   200");
        rejected = sqlRejecter.rejectSQL(originSQL, sqlPattern, "127.0.0.0.1:12345");
        Assert.assertFalse(rejected);

        sqlRejecter.setRejectPattern("not_sqlid_but_length_is_32______  target_id =   200");
        rejected = sqlRejecter.rejectSQL(originSQL, sqlPattern, "127.0.0.0.1:12345");
        Assert.assertFalse(rejected);

        sqlRejecter.setRejectPattern("not_sqlid_but_length_is_32______");
        rejected = sqlRejecter.rejectSQL(originSQL, sqlPattern, "127.0.0.0.1:12345");
        Assert.assertFalse(rejected);

        sqlRejecter
            .setRejectPattern("4c15c759d8707a99b700666e51d0c3ef  target_id = 'is_not_matched'");
        rejected = sqlRejecter.rejectSQL(originSQL, sqlPattern, "127.0.0.0.1:12345");
        Assert.assertFalse(rejected);

        sqlRejecter.setRejectPattern("abc");
        rejected = sqlRejecter.rejectSQL(null, sqlPattern, "127.0.0.0.1:12345");
        Assert.assertFalse(rejected);

        sqlRejecter.setRejectPattern(null);
        rejected = sqlRejecter.rejectSQL(originSQL, sqlPattern, "127.0.0.0.1:12345");
        Assert.assertFalse(rejected);
    }

    @Test public void testRejectLongSQL() throws Exception {
        SQLRejecter sqlRejecter = new SQLRegularExpRejecter("testGroup");
        sqlRejecter.setRejectPattern("abcdefghijk");
        String sql_txt = "ABCDEFGHABCDEFGHABCDEFGHABCDEFGHABCDEFGHABCDEFGHABCDEFGHABCDEFGHABCDEFGH"
            + "ABCDEFGHABCDEFGHABCDEFGHABCDEFGHABCDEFGHABCDEFGHABCDEFGHABCDEFGHABCDEFGHABCDEFGH"
            + "ABCDEFGHABCDEFGHABCDEFGHABCDEFGHABCDEFGHABCDabcdefghijk";
        boolean rejected = sqlRejecter.rejectSQL(sql_txt, "pattern_not_used", "127.0.0.1:12345");
        Assert.assertTrue(rejected);

        sql_txt = "pattern_not_used";
        rejected = sqlRejecter.rejectSQL(sql_txt, "pattern_not_used", "127.0.0.1:12345");
        Assert.assertFalse(rejected);

        sql_txt = "abcdefg";
        sqlRejecter.setRejectPattern("[a-z]*");
        rejected = sqlRejecter.rejectSQL(sql_txt, "pattern_not_used", "127.0.0.1:12345");
        Assert.assertTrue(rejected);

        sqlRejecter.setRejectPattern("**Illegal reject regex");
        rejected = sqlRejecter.rejectSQL(sql_txt, "pattern_not_used", "127.0.0.0.1:12345");
        Assert.assertFalse(rejected);
    }
}
