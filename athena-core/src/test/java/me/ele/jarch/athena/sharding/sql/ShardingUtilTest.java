package me.ele.jarch.athena.sharding.sql;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.parser.SQLParserUtils;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.util.JdbcUtils;
import me.ele.jarch.athena.allinone.DBVendor;
import me.ele.jarch.athena.sharding.ShardingRouter;
import me.ele.jarch.athena.sharding.sql.ShardingUtil.SafeSQL;
import me.ele.jarch.athena.sql.EleMetaParser;
import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by jinghao.wang on 16/7/14.
 */
public class ShardingUtilTest {
    String selectSql =
        "SELECT eleme_order.id FROM eleme_order WHERE eleme_order.created_at >= 1 AND eleme_order.user_id = 100 "
            + "AND eleme_order.restaurant_id IN (200,300) AND distance < 3.0 ORDER BY eleme_order.created_at DESC LIMIT 100";
    String insertSql =
        "insert into eleme_order(id, user_id, restaurant_id, task_hash, date, index) values(1, 2, 3, '21329173', '2016-07-01', 999)";
    String updateSql =
        "update eleme_order Set price = 9.9 where eleme_order.id = 1 AND pre_price = 12.1 AND user_name is not null And rate='b' aNd field1 = 1.2";
    String select1Sql = "select 1";

    String selectSqlWithHex1 = "select 0x1234abcABC";
    String selectSqlWithHex2 = "select X'01234abcABC'";
    String selectSqlWithHex3 = "select x'1234abcABC'";
    String insertSqlWithHex = "insert into test (id, MyIntegerColumn) values (1, '0x1234abcABC')";
    String updateSqlWtihHex = "update test set MyIntegerColumn = 0x1234abcABC where id = 1";

    SQLStatement selectStmt = null;
    SQLStatement insertStmt = null;
    SQLStatement updateStmt = null;
    SQLStatement select1Stmt = null;
    SQLStatement selectSqlWithHex1Stmt = null;
    SQLStatement insertSqlWithHexStmt = null;
    SQLStatement updateSqlWithHexStmt = null;

    SQLStatement selectSqlWithHex2Stmt = null;
    SQLStatement selectSqlWithHex3Stmt = null;

    List<SQLStatement> stmts = new ArrayList<>();

    Predicate<String> whiteFieldsFilter = null;

    @BeforeClass public void init() {
        Set<String> whiteFields = new TreeSet<>();
        whiteFields.add("id");
        whiteFields.add("user_id");
        whiteFields.add("restaurant_id");
        whiteFields.add("task_hash");
        whiteFields.add("field1");
        whiteFields.add("field2");
        whiteFields.add("field3");
        whiteFieldsFilter = whiteFields::contains;
        SQLStatementParser parser =
            SQLParserUtils.createSQLStatementParser(selectSql, JdbcUtils.MYSQL);
        selectStmt = parser.parseStatement();

        SQLStatementParser parser1 =
            SQLParserUtils.createSQLStatementParser(insertSql, JdbcUtils.MYSQL);
        insertStmt = parser1.parseStatement();

        SQLStatementParser parser2 =
            SQLParserUtils.createSQLStatementParser(updateSql, JdbcUtils.MYSQL);
        updateStmt = parser2.parseStatement();

        SQLStatementParser parser3 =
            SQLParserUtils.createSQLStatementParser(select1Sql, JdbcUtils.MYSQL);
        select1Stmt = parser3.parseStatement();

        SQLStatementParser parser4 =
            SQLParserUtils.createSQLStatementParser(selectSqlWithHex1, JdbcUtils.MYSQL);
        selectSqlWithHex1Stmt = parser4.parseStatement();

        SQLStatementParser parser5 =
            SQLParserUtils.createSQLStatementParser(insertSqlWithHex, JdbcUtils.MYSQL);
        insertSqlWithHexStmt = parser5.parseStatement();

        SQLStatementParser parser6 =
            SQLParserUtils.createSQLStatementParser(updateSqlWtihHex, JdbcUtils.MYSQL);
        updateSqlWithHexStmt = parser6.parseStatement();

        SQLStatementParser parser7 =
            SQLParserUtils.createSQLStatementParser(selectSqlWithHex2, JdbcUtils.MYSQL);
        selectSqlWithHex2Stmt = parser7.parseStatement();

        SQLStatementParser parser8 =
            SQLParserUtils.createSQLStatementParser(selectSqlWithHex3, JdbcUtils.MYSQL);
        selectSqlWithHex3Stmt = parser8.parseStatement();
    }

    @Test public void testSafeDebugSQLWhiteFields() throws Exception {
        stmts.add(selectStmt);
        SafeSQL safeSql = ShardingUtil.mostSafeSQLAndWhiteFields(stmts, whiteFieldsFilter);
        System.out.println(safeSql.whiteFields);
        Assert.assertTrue(safeSql.whiteFields.contains("user_id = 100"));
        Assert.assertTrue(safeSql.whiteFields.contains("restaurant_id IN (200, 300)"));
        stmts.clear();
    }

    @Test public void testSafeDebugSQLWhiteFieldsSelect1() throws Exception {
        stmts.add(select1Stmt);
        SafeSQL safeSql = ShardingUtil.mostSafeSQLAndWhiteFields(stmts, whiteFieldsFilter);
        System.out.println(safeSql.whiteFields);
        Assert.assertTrue(safeSql.whiteFields.isEmpty());
        stmts.clear();
    }

    @Test public void testSafeDebugSQLWhiteFieldsMultiSql() throws Exception {
        stmts.add(select1Stmt);
        stmts.add(selectStmt);
        stmts.add(select1Stmt);
        stmts.add(select1Stmt);
        SafeSQL safeSql = ShardingUtil.mostSafeSQLAndWhiteFields(stmts, whiteFieldsFilter);
        System.out.println(safeSql.whiteFields);
        Assert.assertTrue(safeSql.whiteFields.contains("user_id = 100"));
        Assert.assertTrue(safeSql.whiteFields.contains("restaurant_id IN (200, 300)"));
        stmts.clear();
    }

    @Test public void testRemoveIf() throws Exception {
        StringBuilder sb1 = new StringBuilder("#");
        StringBuilder sb2 = new StringBuilder("1#");
        StringBuilder sb3 = new StringBuilder("dsfhdskhf#");
        StringBuilder sb4 = new StringBuilder("sdfhksdhk");
        StringBuilder sb5 = new StringBuilder("sdfhks#dhk");
        StringBuilder sb6 = new StringBuilder("#sdfhks#dhk");
        ShardingUtil.removeIfTailHasString(sb1, "#");
        ShardingUtil.removeIfTailHasString(sb2, "#");
        ShardingUtil.removeIfTailHasString(sb3, "#");
        ShardingUtil.removeIfTailHasString(sb4, "#");
        ShardingUtil.removeIfTailHasString(sb5, "#");
        ShardingUtil.removeIfTailHasString(sb6, "#");
        Assert.assertEquals(sb1.toString(), "");
        Assert.assertEquals(sb2.toString(), "1");
        Assert.assertEquals(sb3.toString(), "dsfhdskhf");
        Assert.assertEquals(sb4.toString(), "sdfhksdhk");
        Assert.assertEquals(sb5.toString(), "sdfhks#dhk");
        Assert.assertEquals(sb6.toString(), "#sdfhks#dhk");
    }

    @Test public void testSafeLogHexHiden() {
        stmts.add(selectSqlWithHex1Stmt);
        stmts.add(selectSqlWithHex2Stmt);
        stmts.add(selectSqlWithHex3Stmt);
        stmts.add(insertSqlWithHexStmt);
        stmts.add(updateSqlWithHexStmt);

        SafeSQL safeSql = ShardingUtil.mostSafeSQLAndWhiteFields(stmts, whiteFieldsFilter);
        System.out.println(safeSql.mostSafeSql);
        System.out.println(safeSql.whiteFields);
        Assert.assertFalse(safeSql.mostSafeSql.contains("1234"));

    }

    @Test public void testMultiInsertSamePattern() {
        String singleInsert =
            "insert into eos_mysql_task(id,user_id,restaurant_id,task_hash) values (1,2,3,'test_task_hash')";
        StringBuilder multiInsertSb = new StringBuilder();
        IntStream.range(0, 20).forEach(i -> multiInsertSb.append(singleInsert).append(";"));
        String multiInsert = multiInsertSb.toString();
        SQLStatementParser multiParser =
            SQLParserUtils.createSQLStatementParser(multiInsert, JdbcUtils.MYSQL);
        List<SQLStatement> multiStatements = multiParser.parseStatementList();
        SafeSQL safeSQL =
            ShardingUtil.mostSafeSQLAndWhiteFields(multiStatements, whiteFieldsFilter);
        Assert.assertEquals(safeSQL.mostSafeSql,
            "INSERT INTO eos_mysql_task (id, user_id, restaurant_id, task_hash) VALUES (?)_x_N");
    }

    @Test public void testMultiSQLDifferPattern() {
        String multiSQL =
            "insert into eos_mysql_task(id,user_id,restaurant_id,task_hash) values (1,2,3,'test_task_hash');select * from eleme_order where id = 234";
        SQLStatementParser multiParser =
            SQLParserUtils.createSQLStatementParser(multiSQL, JdbcUtils.MYSQL);
        List<SQLStatement> multiStatements = multiParser.parseStatementList();
        SafeSQL safeSQL =
            ShardingUtil.mostSafeSQLAndWhiteFields(multiStatements, whiteFieldsFilter);
        Assert.assertEquals(safeSQL.mostSafeSql,
            "INSERT INTO eos_mysql_task (id, user_id, restaurant_id, task_hash) VALUES (?);SELECT * FROM eleme_order WHERE id = ?");
    }

    @Test public void testSingleInsertPattern() {
        String singleInsert =
            "insert into eos_mysql_task(id,user_id,restaurant_id,task_hash) values (1,2,3,'test_task_hash')";
        SQLStatementParser singleParser =
            SQLParserUtils.createSQLStatementParser(singleInsert, JdbcUtils.MYSQL);
        List<SQLStatement> singleStatements = singleParser.parseStatementList();
        SafeSQL safeSQL =
            ShardingUtil.mostSafeSQLAndWhiteFields(singleStatements, whiteFieldsFilter);
        Assert.assertEquals(safeSQL.mostSafeSql,
            "INSERT INTO eos_mysql_task (id, user_id, restaurant_id, task_hash) VALUES (?)");
    }

    @Test public void testEmptySQLPattern() {
        String singleInsert = "   ";
        SQLStatementParser singleParser =
            SQLParserUtils.createSQLStatementParser(singleInsert, JdbcUtils.MYSQL);
        List<SQLStatement> singleStatements = singleParser.parseStatementList();
        SafeSQL safeSQL =
            ShardingUtil.mostSafeSQLAndWhiteFields(singleStatements, whiteFieldsFilter);
        Assert.assertEquals(safeSQL.mostSafeSql, "");
        Assert.assertEquals(safeSQL.whiteFields, "");
    }

    /**
     * 此单元测试用于确保在druid升级时避免出坑，默认情况是禁掉此单元测试的，只有在druid升级时才打开运行。
     * 具体做法:
     * 抽样DAL机器开启unsafe.log, 将unsafe.log.* 压缩后下载到自己电脑本地后解压到某个目录
     * 修改代码中的{@code unsafeLogDir} 变量为unsafe.log的解压目录。
     * 将单元测试的enable 改为 {@code true}, 运行单元测试。
     * 此单元测试将会先读取测试根目录下的mysql_archive_sqls.txt和postgresql_archive_sqls.txt文件,读取历史已经
     * 记录过的SQL hash及其样例。 然后读取unsafeLogDir下的unsafe.log*文件，过滤未在mysql_archive_sqls.txt
     * 和postgresql_archive_sqls.txt文件中记录的SQL hash及其样例。合并存量的sql hash及在unsafe.log中的增量
     * SQL hash。 对每种SQL hash的样例进行druid语法解析器校验，未能通过简单规则(原始SQL移除所有空格并转换为小写，
     * druid生成的SQL移除所有空格并转换为小写, assert两者的字符串完全相同)校验通过的SQL 将会写入到本机磁盘的
     * /tmp/manual_check_sqls.txt文件，需要人工判断经过druid生成的SQL是否改变了原始SQL的语义。
     * 结果:
     * 通过上述方式，mysql_archive_sqls.txt和postgresql_archive_sqls.txt中的SQL样式将会越来越多，我们对与druid
     * 语法库的升级把握也更大。
     *
     * @throws Exception
     */
    @Test(enabled = false) public void unsafeLog() throws Exception {
        String unsafeLogDir = "/Users/whisper/Desktop/test/unsafe/prod";
        Path path = Paths.get(unsafeLogDir);
        Path mysqlArchivedSqls = Paths.get("src/test", "mysql_archive_sqls.txt");
        Path postgreSqlsArchivedSqls = Paths.get("src/test", "postgresql_archive_sqls.txt");
        Path manualCheckSQLs = Paths.get("/tmp", "manual_check_sqls.txt");
        Files.deleteIfExists(manualCheckSQLs);
        Files.createFile(manualCheckSQLs);
        final Map<String, String> postgreSQLPatternWithSamples =
            new ConcurrentSkipListMap<>(readArchivedSqls(postgreSqlsArchivedSqls));
        final Map<String, String> mySQLPatternWithSamples =
            new ConcurrentSkipListMap<>(readArchivedSqls(mysqlArchivedSqls));
        final Pattern pattern = Pattern.compile("\\[(.*)\\]");
        final LongAdder counter = new LongAdder();
        if (Files.isDirectory(path)) {
            Files.newDirectoryStream(path, fp -> fp.toString().contains("unsafe.log"))
                .forEach(p -> {
                    System.out.println(String.format("read log: %s start", p.toString()));
                    long start = System.currentTimeMillis();
                    try (BufferedReader br = Files.newBufferedReader(p, StandardCharsets.UTF_8)) {
                        br.lines().filter(l -> l.contains("UnsafeLog")).filter(
                            l -> l.contains("no sharding") || l.contains("Origin sharded sql"))
                            .forEach(l -> {
                                Matcher matcher = pattern.matcher(l);
                                counter.increment();
                                if (!matcher.find()) {
                                    return;
                                }
                                String rawSQL = matcher.group(1);
                                EleMetaParser parser = EleMetaParser.parse(rawSQL);
                                String query = parser.getQueryWithoutComment();
                                String comment = parser.getQueryComment();
                                if (l.contains("_pggroup")) {
                                    ShardingSQL shardingSQL = ShardingSQL
                                        .handlePGBasicly(query, comment,
                                            ShardingRouter.defaultShardingRouter,
                                            new Send2BatchCond(), ShardingSQL.BLACK_FIELDS_FILTER);
                                    postgreSQLPatternWithSamples
                                        .putIfAbsent(shardingSQL.originMostSafeSQL, rawSQL);
                                } else {
                                    ShardingSQL shardingSQL = ShardingSQL
                                        .handleMySQLBasicly(query, comment,
                                            ShardingRouter.defaultShardingRouter,
                                            new Send2BatchCond(), ShardingSQL.BLACK_FIELDS_FILTER);
                                    mySQLPatternWithSamples
                                        .putIfAbsent(shardingSQL.originMostSafeSQL, rawSQL);
                                }
                            });
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    System.out.println("read: " + counter.longValue() + " log in mills: " + (
                        System.currentTimeMillis() - start));
                });
        }

        final LongAdder postgreSQLStrictEquals = new LongAdder();
        postgreSQLPatternWithSamples.forEach((p, sample) -> {
            if (checkSQLLogicEquals(p, sample, DBVendor.PG, manualCheckSQLs)) {
                postgreSQLStrictEquals.increment();
            }
        });
        System.out.println(String
            .format("asserted: %d PostgreSQL SQLs, %d sqls strict equal, %d sqls need manual check",
                postgreSQLPatternWithSamples.size(), postgreSQLStrictEquals.longValue(),
                (postgreSQLPatternWithSamples.size() - postgreSQLStrictEquals.longValue())));
        final LongAdder mySQLStrictEquals = new LongAdder();
        mySQLPatternWithSamples.forEach((p, sample) -> {
            if (checkSQLLogicEquals(p, sample, DBVendor.MYSQL, manualCheckSQLs)) {
                mySQLStrictEquals.increment();
            }
        });
        System.out.println(String
            .format("asserted: %d MySQL SQLs, %d sqls strict equal, %d sqls need manual check",
                mySQLPatternWithSamples.size(), mySQLStrictEquals.longValue(),
                (mySQLPatternWithSamples.size() - mySQLStrictEquals.longValue())));
        try (BufferedWriter bw = Files
            .newBufferedWriter(mysqlArchivedSqls, StandardCharsets.UTF_8)) {
            mySQLPatternWithSamples.forEach((p, s) -> {
                try {
                    bw.write(p + "|" + s);
                    bw.newLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
        try (BufferedWriter bw = Files
            .newBufferedWriter(postgreSqlsArchivedSqls, StandardCharsets.UTF_8)) {
            postgreSQLPatternWithSamples.forEach((p, s) -> {
                try {
                    bw.write(p + "|" + s);
                    bw.newLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
    }

    private Map<String, String> readArchivedSqls(Path path) {
        if (!Files.exists(path)) {
            return Collections.emptyMap();
        }
        try (BufferedReader br = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            return br.lines().filter(l -> l.contains("|")).map(l -> l.split("\\|", 2))
                .collect(Collectors.toMap(sl -> sl[0], sl -> sl[1]));
        } catch (IOException e) {
            return Collections.emptyMap();
        }
    }

    private boolean checkSQLLogicEquals(final String pattern, final String sample,
        final DBVendor vendor, final Path manualCheckSQLsPath) {
        EleMetaParser parser = EleMetaParser.parse(sample);
        String query = parser.getQueryWithoutComment();
        String comment = parser.getQueryComment();

        ShardingSQL shardingSQL = null;
        if (DBVendor.PG == vendor) {
            shardingSQL = ShardingSQL
                .handlePGBasicly(query, comment, ShardingRouter.defaultShardingRouter,
                    new Send2BatchCond(), ShardingSQL.BLACK_FIELDS_FILTER);
        } else if (DBVendor.MYSQL == vendor) {
            shardingSQL = ShardingSQL
                .handleMySQLBasicly(query, comment, ShardingRouter.defaultShardingRouter,
                    new Send2BatchCond(), ShardingSQL.BLACK_FIELDS_FILTER);
        } else {
            Assert
                .fail("unknown db vendor: " + vendor + " pattern:" + pattern + " sample:" + sample);
        }
        // assert 样例生成的pattern和预期的pattern一致
        Assert.assertEquals(shardingSQL.mostSafeSQL, pattern);

        String debugedSQL = ShardingUtil.debugSql(shardingSQL.sqlStmt);
        // 移除所有空白符
        String actual = debugedSQL.replaceAll("\\s+", "");
        actual = actual.trim().toLowerCase();

        String expected = query.replaceAll("\\s+", "");
        expected = expected.trim().toLowerCase();

        if (Objects.equals(actual, expected)) {
            return true;
        } else {
            String commonPrefix = longestCommonPrefix(expected, actual);
            List<String> outputs = new LinkedList<>();
            outputs.add(String
                .format("NOT EQUAL SQL with pattern:%s, sample:%s, vendor:%s", pattern, sample,
                    vendor.name()));
            outputs
                .add("expect diff:" + expected.substring(commonPrefix.length(), expected.length()));
            outputs.add("actual diff:" + actual.substring(commonPrefix.length(), actual.length()));
            try {
                Files.write(manualCheckSQLsPath, outputs, StandardCharsets.UTF_8,
                    StandardOpenOption.APPEND, StandardOpenOption.WRITE);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return false;
        }
    }

    private static String longestCommonPrefix(String a, String b) {
        if (StringUtils.isEmpty(a) || StringUtils.isEmpty(b)) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < Math.min(a.length(), b.length()); i++) {
            if (a.charAt(i) == b.charAt(i)) {
                sb.append(a.charAt(i));
            } else {
                break;
            }
        }
        return sb.toString();
    }
}
