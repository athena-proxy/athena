package me.ele.jarch.athena.util.log;

import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * @author jinghao.wang
 */
public class RawSQLFilterTaskTest {
    @Test public void sysInfoSQL() {
        assertTrue(RawSQLFilterTask.FILTERS.get(0)
            .test("select a, b from information_schema where 1 = 1"));
        assertFalse(RawSQLFilterTask.FILTERS.get(1)
            .test("select a, b from information_schema where 1 = 1"));
        assertTrue(RawSQLFilterTask.FILTERS.get(4)
            .test("select a, b from information_schema where 1 = 1"));
    }

    @Test public void delaySQL() {
        assertTrue(RawSQLFilterTask.FILTERS.get(1)
            .test("select sleep() from information_schema where 1 = 0"));
        assertTrue(RawSQLFilterTask.FILTERS.get(4)
            .test("select sleep() from information_schema where 1 = 0"));
    }

    @Test public void singleQuota() {
        assertTrue(RawSQLFilterTask.FILTERS.get(2)
            .test("select id from users where name = 'x' and 1=1 --'"));
    }

    @Test public void comment() {
        assertTrue(RawSQLFilterTask.FILTERS.get(3).test(
            "select id from users where name = '1' UNION/*!0 select user,2,3,4,5,6/*!0from/*!0mysql.user/*-"));
    }

    @Test public void dangerFunction() {
        assertTrue(RawSQLFilterTask.FILTERS.get(3)
            .test("select id from users where name = '1' and left(database(), 2) > 'sa' --+"));
    }

    @Test public void attack() {
        assertTrue(RawSQLFilterTask.FILTERS.get(8).test(
            "select id from users where name = -000000063 union select 1,2,3,4,group_concat(username, chat(58), password),6,7,8,9 from step_users"));
    }
}
