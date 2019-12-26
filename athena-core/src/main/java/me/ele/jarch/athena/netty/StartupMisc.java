package me.ele.jarch.athena.netty;

import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.netty.debug.SystemInfo;

import java.io.File;
import java.util.Date;

public class StartupMisc {

    public static void doPrepareStartup() {
        addShutdownHook();
    }

    private static void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

            @Override public void run() {
                System.out.println("System is exiting ... on " + new Date());
                System.out.println(SystemInfo.getInfo());
                deleteDebugPortFile();
            }
        }));
    }

    private static void deleteDebugPortFile() {
        File file = new File(Constants.DAL_DEBUG_SERVER_FILE);
        if (file.isFile() && file.exists()) {
            file.delete();
            System.out.println("Success. delete file=" + Constants.DAL_DEBUG_SERVER_FILE);
        }
    }
}
