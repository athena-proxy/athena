package me.ele.jarch.athena.util.log;

import com.google.re2j.Pattern;
import me.ele.jarch.athena.constant.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

/**
 * @author jinghao.wang
 */
public class RawSQLFilterTask implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(RawSQLFilterTask.class);
    public static final List<RawSQLFilter> FILTERS;
    private final RawSQLContxt container;

    public RawSQLFilterTask(RawSQLContxt container) {
        this.container = container;
    }

    @Override public void run() {
        int score = 0;
        for (RawSQLFilter filter : FILTERS) {
            try {
                if (filter.test(container.sqlWithoutComment())) {
                    score += filter.score();
                }
                if (score >= 8) {
                    break;
                }
            } catch (Exception ignore) {
            }
        }
        if (score >= 8) {
            logSQL(container);
        }
    }

    private void logSQL(RawSQLContxt container) {
        StringBuilder sb = new StringBuilder(Constants.TYPICAL_SQL_SIZE);
        sb.append(container.dalGroup()).append("=>").append(container.user()).append("=>")
            .append(container.clientInfo()).append("=>").append(container.transactionId())
            .append("=>").append('[').append(container.rawSQL()).append(']');
        LOGGER.warn(sb.toString());
    }

    static class RawSQLFilter implements Predicate<String> {
        private final Pattern pattern;
        private final String description;
        private final int score;

        public RawSQLFilter(Pattern pattern, String description, int score) {
            this.pattern = pattern;
            this.description = description;
            this.score = score;
        }

        @Override public boolean test(String s) {
            return pattern.matcher(s).find();
        }

        public String description() {
            return description;
        }

        public int score() {
            return score;
        }
    }


    static {
        List<RawSQLFilter> init = new ArrayList<>(10);
        init.add(new RawSQLFilter(Pattern.compile(
            "^(?P<res>.*)information_schema|system_user|@@version|@@version_compile_os|@@basedir|session_user|current_user|current_db|version_compile_os|mysql\\.",
            Pattern.DOTALL), "mysql系统信息", 4));
        init.add(new RawSQLFilter(
            Pattern.compile("^(?P<res>.*)sleep|benchmark|receive_message|pg_sleep", Pattern.DOTALL),
            "延时关键词 开发不可能使用", 8));
        init.add(new RawSQLFilter(
            Pattern.compile("^(?P<res>.*)\\-\\-\\s*['\"]{1}[^'\"]*$", Pattern.DOTALL),
            "攻击中用到 在注释符号后有未闭合的引号", 8));
        init.add(
            new RawSQLFilter(Pattern.compile("^(?P<res>.*)/\\*|/\\*?!\\d+|\\-\\-", Pattern.DOTALL),
                "注释", 4));
        init.add(new RawSQLFilter(Pattern
            .compile("^(?P<res>.*)['\"]*\\d+['|\"]*\\s*(=|>|<|>=|<=)\\s*['|\"]*\\d+['|\"]*",
                Pattern.DOTALL), "永真或永假的条件", 8));
        init.add(new RawSQLFilter(
            Pattern.compile("^(?P<res>.*)(and|or)+\\s+\\d+\\s*[^=]+", Pattern.DOTALL), "永真或永假的条件",
            8));
        init.add(new RawSQLFilter(Pattern.compile(
            "^(?P<res>.*)\\b(elt|chr|substring|ascii|truncate|load_file|updatexml|extractvalue|xmltype|is_srvrolemember|char|strcmp|substr|bin|ord|hex|database)+\\(",
            Pattern.DOTALL), "攻击中常用，开发不常用函数", 4));
        init.add(new RawSQLFilter(Pattern.compile("^(?P<res>.*)\\||\\^|~|xor", Pattern.DOTALL),
            "mysql系统信息", 4));
        init.add(new RawSQLFilter(
            Pattern.compile("^(?P<res>.*)select\\s*\\d+\\s*,\\s*\\d+\\s*", Pattern.DOTALL),
            "出现两个以上数字加逗号组合即可认为是攻击，常用于union查询", 8));
        init.add(new RawSQLFilter(Pattern.compile("^(?P<res>.*)(\\b|'|\")0x", Pattern.DOTALL),
            "包含16进制字符串", 4));
        FILTERS = Collections.unmodifiableList(init);
    }
}
