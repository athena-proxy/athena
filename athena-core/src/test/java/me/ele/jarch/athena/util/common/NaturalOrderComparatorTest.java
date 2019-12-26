package me.ele.jarch.athena.util.common;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

public class NaturalOrderComparatorTest {

    @Test public void compare() {
        List<String> strs = new ArrayList<>();
        strs.add("shd_eleme_order_uid200");
        strs.add("shd_eleme_order_uid100");
        strs.add("shd_eleme_order_uida");
        strs.add("shd_eleme_order_uid0");
        strs.add("shd_eleme_order_uid1");
        strs.add("shd_eleme_order_uid2");
        strs.add("shd_eleme_order_uid3");
        strs.add("shd_eleme_order_uid10");
        strs.add("shd_eleme_order_uid11");
        strs.add("shd_eleme_order_uid12");
        strs.add("shd_eleme_order_uid20");
        strs.add("shd_eleme_order_uid21");
        strs.add("shd_eleme_order_uid22");
        strs.sort(new NaturalOrderComparator());
        List<String> result = new ArrayList<>();
        result.add("shd_eleme_order_uid0");
        result.add("shd_eleme_order_uid1");
        result.add("shd_eleme_order_uid2");
        result.add("shd_eleme_order_uid3");
        result.add("shd_eleme_order_uid10");
        result.add("shd_eleme_order_uid11");
        result.add("shd_eleme_order_uid12");
        result.add("shd_eleme_order_uid20");
        result.add("shd_eleme_order_uid21");
        result.add("shd_eleme_order_uid22");
        result.add("shd_eleme_order_uid100");
        result.add("shd_eleme_order_uid200");
        result.add("shd_eleme_order_uida");
        Assert.assertEquals(strs, result);
    }
}
