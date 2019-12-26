package me.ele.jarch.athena.util;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Created by donxuu on 9/18/15.
 */
public class ConfigFileReloaderTest {

    @Test public void testLoad4FirstTime() throws InterruptedException {
        ConfigFileReloader reloader = new ConfigFileReloader("/etc/passwd", i -> loadConfig(i));
        boolean modified = reloader.tryLoad();
        Assert.assertEquals(modified, true);
    }

    private static void loadConfig(String file) {

    }

}
