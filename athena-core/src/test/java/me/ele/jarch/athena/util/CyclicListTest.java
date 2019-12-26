package me.ele.jarch.athena.util;

import me.ele.jarch.athena.server.async.MasterHeartBeat;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Created by zhengchao on 16/7/7.
 */
public class CyclicListTest {

    @Test public void testAdd() {
        CyclicList<MasterHeartBeat.STATUS> list = new CyclicList<>(20);
        list.add(MasterHeartBeat.STATUS.SUCCESS);
        list.add(MasterHeartBeat.STATUS.SUCCESS);
        list.add(MasterHeartBeat.STATUS.SUCCESS);
        list.add(MasterHeartBeat.STATUS.FAILED);
        list.add(MasterHeartBeat.STATUS.FAILED);
        list.add(MasterHeartBeat.STATUS.FAILED);

        List<MasterHeartBeat.STATUS> contents = list.takeAll();
        Assert.assertEquals(contents.size(), 6);
        Assert.assertEquals(contents.get(0), MasterHeartBeat.STATUS.SUCCESS);
        Assert.assertEquals(contents.get(5), MasterHeartBeat.STATUS.FAILED);
    }

    @Test public void testWrapping01() {
        CyclicList<MasterHeartBeat.STATUS> list = new CyclicList<>(4);
        list.add(MasterHeartBeat.STATUS.SUCCESS);
        list.add(MasterHeartBeat.STATUS.SUCCESS);
        list.add(MasterHeartBeat.STATUS.SUCCESS);
        list.add(MasterHeartBeat.STATUS.FAILED);
        list.add(MasterHeartBeat.STATUS.FAILED);
        list.add(MasterHeartBeat.STATUS.FAILED);
        list.add(MasterHeartBeat.STATUS.FAILED);

        List<MasterHeartBeat.STATUS> contents = list.takeAll();
        Assert.assertEquals(contents.size(), 4);
        for (MasterHeartBeat.STATUS status : contents) {
            Assert.assertEquals(status, MasterHeartBeat.STATUS.FAILED);
        }
    }

    @Test public void testWrapping02() {
        CyclicList<MasterHeartBeat.STATUS> list = new CyclicList<>(4);
        list.add(MasterHeartBeat.STATUS.SUCCESS);
        list.add(MasterHeartBeat.STATUS.SUCCESS);
        list.add(MasterHeartBeat.STATUS.SUCCESS);
        list.add(MasterHeartBeat.STATUS.FAILED);
        list.add(MasterHeartBeat.STATUS.FAILED);

        List<MasterHeartBeat.STATUS> contents = list.takeAll();
        Assert.assertEquals(contents.size(), 4);
        Assert.assertEquals(contents.get(0), MasterHeartBeat.STATUS.FAILED);
        Assert.assertEquals(contents.get(1), MasterHeartBeat.STATUS.SUCCESS);
        Assert.assertEquals(contents.get(2), MasterHeartBeat.STATUS.SUCCESS);
        Assert.assertEquals(contents.get(3), MasterHeartBeat.STATUS.FAILED);
    }
}
