package me.ele.jarch.athena.util.etrace;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class EtracePatternTest {

    @Test public void testCreateEmptyProducer() throws Exception {

        String sqlPattern = "me.ele.arch.das.misc_gzs_init";
        EtracePatternUtil.SQLRecord sqlRecord =
            EtracePatternUtil.addAndGet("me.ele.arch.das.misc_gzs_init");

        assertEquals(sqlRecord.sqlPattern, sqlPattern);
        assertEquals(sqlRecord.hash, DigestUtils.md5Hex(sqlPattern));

        String sqlInfo = EtracePatternUtil.sqlPatternInfo();
        assertTrue(!StringUtils.isEmpty(sqlInfo) && sqlInfo.contains(sqlPattern));
    }
}
