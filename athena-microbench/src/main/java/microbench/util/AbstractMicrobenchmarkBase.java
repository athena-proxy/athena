package microbench.util;

import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.testng.annotations.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertNull;

/**
 * Created by jinghao.wang on 2018/6/14.
 */
@Warmup(iterations = AbstractMicrobenchmarkBase.DEFAULT_WARMUP_ITERATIONS)
@Measurement(iterations = AbstractMicrobenchmarkBase.DEFAULT_MEASURE_ITERATIONS)
@State(Scope.Thread) public abstract class AbstractMicrobenchmarkBase {
    protected static final int DEFAULT_WARMUP_ITERATIONS = 10;
    protected static final int DEFAULT_MEASURE_ITERATIONS = 10;
    protected static final String[] BASE_JVM_ARGS =
        {"-server", "-dsa", "-da", "-XX:+AggressiveOpts", "-XX:+UseBiasedLocking",
            "-XX:+UseFastAccessorMethods", "-XX:+OptimizeStringConcat",
            "-XX:+HeapDumpOnOutOfMemoryError"};

    protected ChainedOptionsBuilder newOptionsBuilder() throws Exception {
        String className = getClass().getSimpleName();

        ChainedOptionsBuilder runnerOptions =
            new OptionsBuilder().include(".*" + className + ".*").jvmArgs(jvmArgs());

        if (getWarmupIterations() > 0) {
            runnerOptions.warmupIterations(getWarmupIterations());
        }

        if (getMeasureIterations() > 0) {
            runnerOptions.measurementIterations(getMeasureIterations());
        }

        if (getReportDir() != null) {
            String filePath = getReportDir() + className + ".json";
            File file = new File(filePath);
            if (file.exists()) {
                file.delete();
            } else {
                file.getParentFile().mkdirs();
                file.createNewFile();
            }

            runnerOptions.resultFormat(ResultFormatType.JSON);
            runnerOptions.result(filePath);
        }

        return runnerOptions;
    }

    protected abstract String[] jvmArgs();

    protected static String[] removeAssertions(String[] jvmArgs) {
        List<String> customArgs = new ArrayList<String>(jvmArgs.length);
        for (String arg : jvmArgs) {
            if (!arg.startsWith("-ea")) {
                customArgs.add(arg);
            }
        }
        if (jvmArgs.length != customArgs.size()) {
            jvmArgs = new String[customArgs.size()];
            customArgs.toArray(jvmArgs);
        }
        return jvmArgs;
    }

    @Test public void run() throws Exception {
        new Runner(newOptionsBuilder().build()).run();
    }

    protected int getWarmupIterations() {
        return Integer.valueOf(System.getProperty("warmupIterations", "-1").trim());
    }

    protected int getMeasureIterations() {
        return Integer.valueOf(System.getProperty("measureIterations", "-1").trim());
    }

    protected String getReportDir() {
        return System.getProperty("perfReportDir");
    }

    public static void handleUnexpectedException(Throwable t) {
        assertNull(t);
    }
}
