package me.ele.jarch.athena.netty;

import ch.qos.logback.classic.AsyncAppender;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.Configurator;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.rolling.FixedWindowRollingPolicy;
import ch.qos.logback.core.rolling.SizeBasedTriggeringPolicy;
import ch.qos.logback.core.spi.ContextAwareBase;
import ch.qos.logback.core.util.FileSize;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.util.AthenaFileAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;


/**
 * Created by zhengchao on 16/6/17.
 */
public class AthenaUtils {
    private static Logger logger = LoggerFactory.getLogger(AthenaUtils.class);

    private static String externalCommand(String[] args) {
        String result = null;
        try {
            Process p = Runtime.getRuntime().exec(args);
            BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
            result = br.readLine();

            p.destroy();
        } catch (IOException e) {
            logger.error("failed to execute external command: " + String.join(" ", args), e);
        }

        return result;
    }

    public static int getPPid() {
        String ppidStr = externalCommand(new String[] {"perl", "-e", "print getppid(). \"\n\";"});

        if (ppidStr == null) {
            return 0;
        }

        return Integer.parseInt(ppidStr);
    }

    /*
     * TODO
     * This can be removed once our server all have been migrated to kernel which is above 3.9.
     * */
    public static boolean isOSKernelSupportReuseport() {
        String OsType = System.getProperty("os.name");
        if (!OsType.contains("Linux")) {
            return false;
        }

        String versionStr = System.getProperty("os.version");

        String[] versions = versionStr.split("\\.");
        int major = Integer.parseInt(versions[0]);
        int minor = Integer.parseInt(versions[1]);

        return major > 3 || major == 3 && minor >= 9;
    }

    public static long getFileCreationTime(Path p) throws IOException {
        BasicFileAttributes attr;
        attr = Files.readAttributes(p, BasicFileAttributes.class);
        return attr.creationTime().toMillis();
    }

    public static String pidStr() {
        return "Athena (Pid: " + Constants.PID + ")";
    }

    static class ReConfigurator extends ContextAwareBase implements Configurator {
        public void configure(LoggerContext lc) {
            addInfo("reconfig");

            // file appender
            AthenaFileAppender<ILoggingEvent> fileAppender =
                new AthenaFileAppender<ILoggingEvent>();
            fileAppender.setName("file");
            fileAppender.setFile("/data/log/" + Constants.APPID + "/athena_old.log");
            fileAppender.setContext(lc);

            FixedWindowRollingPolicy policy = new FixedWindowRollingPolicy();
            policy.setMaxIndex(20);
            policy.setMinIndex(1);
            policy.setContext(lc);
            policy.setParent(fileAppender);
            policy.setFileNamePattern("/data/log/" + Constants.APPID + "/athena_old.log.%i");
            policy.start();

            SizeBasedTriggeringPolicy<ILoggingEvent> triggeringPolicy =
                new SizeBasedTriggeringPolicy<>();
            triggeringPolicy.setMaxFileSize(new FileSize(2 * FileSize.GB_COEFFICIENT));
            triggeringPolicy.setContext(lc);
            triggeringPolicy.start();

            fileAppender.setRollingPolicy(policy);
            fileAppender.setTriggeringPolicy(triggeringPolicy);


            PatternLayoutEncoder filePl = new PatternLayoutEncoder();
            filePl.setPattern("%d{yyyy-MM-dd HH:mm:ss.SSS} %le %logger{0}: ## %msg %ex\\n");
            filePl.setContext(lc);
            filePl.start();
            fileAppender.setEncoder(filePl);

            fileAppender.start();

            AsyncAppender async = new AsyncAppender();
            async.setContext(lc);
            async.addAppender(fileAppender);
            async.setName("ASYNC");
            async.start();

            ch.qos.logback.classic.Logger rootLogger = lc.getLogger(Logger.ROOT_LOGGER_NAME);
            rootLogger.setLevel(Level.INFO);
            rootLogger.addAppender(async);
        }
    }

    public static void configLogbackToOldLog() {
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        ReConfigurator cfg = new ReConfigurator();
        cfg.setContext(lc);
        lc.reset();
        cfg.configure(lc);
    }
}
