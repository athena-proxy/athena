package microbench.util;

import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;

/**
 * Created by jinghao.wang on 2018/6/14.
 */
@Fork(AbstractMicrobenchmark.DEFAULT_FORKS) public abstract class AbstractMicrobenchmark
    extends AbstractMicrobenchmarkBase {
    protected static final int DEFAULT_FORKS = 2;

    private final String[] jvmArgs;

    public AbstractMicrobenchmark() {
        this(false);
    }

    public AbstractMicrobenchmark(boolean disableAssertions) {
        final String[] customArgs =
            new String[] {"-Xms768m", "-Xmx768m", "-XX:MaxDirectMemorySize=768m"};
        String[] jvmArgs = new String[BASE_JVM_ARGS.length + customArgs.length];
        System.arraycopy(BASE_JVM_ARGS, 0, jvmArgs, 0, BASE_JVM_ARGS.length);
        System.arraycopy(customArgs, 0, jvmArgs, BASE_JVM_ARGS.length, customArgs.length);
        if (disableAssertions) {
            jvmArgs = removeAssertions(jvmArgs);
        }
        this.jvmArgs = jvmArgs;
    }

    @Override protected String[] jvmArgs() {
        return jvmArgs;
    }

    @Override protected ChainedOptionsBuilder newOptionsBuilder() throws Exception {
        ChainedOptionsBuilder runnerOptions = super.newOptionsBuilder();
        if (getForks() > 0) {
            runnerOptions.forks(getForks());
        }

        return runnerOptions;
    }

    protected int getForks() {
        return Integer.valueOf(System.getProperty("forks", "-1").trim());
    }
}
