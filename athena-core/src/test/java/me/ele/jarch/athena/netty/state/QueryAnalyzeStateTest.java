package me.ele.jarch.athena.netty.state;

import com.github.mpjct.jmpjct.mysql.proto.Com_Query;
import me.ele.jarch.athena.netty.MySqlClientPacketDecoder;
import me.ele.jarch.athena.netty.SqlSessionContext;
import me.ele.jarch.athena.sql.CmdQuery;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Created by jinghao.wang on 15/12/29.
 */
public class QueryAnalyzeStateTest {

    @Test public void testParseSetSql() throws Exception {
        SqlSessionContext sqlCtx =
            new SqlSessionContext(new MySqlClientPacketDecoder(false, "1.1.1.1"), "1.1.1.1");
        QueryAnalyzeState queryAnalyzeState = new QueryAnalyzeState(sqlCtx);

        Com_Query comQuery = new Com_Query();
        comQuery.setQuery("set autocommit=1");
        comQuery.sequenceId = 2;
        CmdQuery cmdQuery = new CmdQuery(comQuery.toPacket(), "eosgroup");
        cmdQuery.execute();
        sqlCtx.transactionController.setAutoCommit(false);
        queryAnalyzeState.executeSetAutoCommitCmd(cmdQuery);
        Assert.assertTrue(sqlCtx.transactionController.isAutoCommit());

        Com_Query comQuery1 = new Com_Query();
        comQuery1.setQuery("set @@autocommit=1");
        comQuery1.sequenceId = 2;
        CmdQuery cmdQuery1 = new CmdQuery(comQuery1.toPacket(), "eosgroup");
        cmdQuery1.execute();
        sqlCtx.transactionController.setAutoCommit(false);
        queryAnalyzeState.executeSetAutoCommitCmd(cmdQuery1);
        Assert.assertTrue(sqlCtx.transactionController.isAutoCommit());

        Com_Query comQuery2 = new Com_Query();
        comQuery2.setQuery("SET @@autocommit =1");
        comQuery2.sequenceId = 2;
        CmdQuery cmdQuery2 = new CmdQuery(comQuery2.toPacket(), "eosgroup");
        cmdQuery2.execute();
        sqlCtx.transactionController.setAutoCommit(false);
        queryAnalyzeState.executeSetAutoCommitCmd(cmdQuery2);
        Assert.assertTrue(sqlCtx.transactionController.isAutoCommit());

        Com_Query comQuery3 = new Com_Query();
        comQuery3.setQuery("set @@AUTOCOMMIT= 0");
        comQuery.sequenceId = 2;
        CmdQuery cmdQuery3 = new CmdQuery(comQuery3.toPacket(), "eosgroup");
        cmdQuery3.execute();
        sqlCtx.transactionController.setAutoCommit(true);
        queryAnalyzeState.executeSetAutoCommitCmd(cmdQuery3);
        Assert.assertFalse(sqlCtx.transactionController.isAutoCommit());

        Com_Query comQuery4 = new Com_Query();
        comQuery4.setQuery("SET @@AUTOCOMMIT = 0");
        comQuery4.sequenceId = 2;
        CmdQuery cmdQuery4 = new CmdQuery(comQuery4.toPacket(), "eosgroup");
        cmdQuery4.execute();
        sqlCtx.transactionController.setAutoCommit(false);
        queryAnalyzeState.executeSetAutoCommitCmd(cmdQuery4);
        Assert.assertFalse(sqlCtx.transactionController.isAutoCommit());

        Com_Query comQuery5 = new Com_Query();
        comQuery5.setQuery("SET CHARACTER SET cp1251_koi8");
        comQuery5.sequenceId = 2;
        CmdQuery cmdQuery5 = new CmdQuery(comQuery5.toPacket(), "eosgroup");
        cmdQuery5.execute();
        sqlCtx.transactionController.setAutoCommit(false);
        queryAnalyzeState.executeSetAutoCommitCmd(cmdQuery5);
        Assert.assertFalse(sqlCtx.transactionController.isAutoCommit());
    }
}
