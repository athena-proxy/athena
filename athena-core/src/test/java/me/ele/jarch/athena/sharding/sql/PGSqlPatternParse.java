package me.ele.jarch.athena.sharding.sql;

import me.ele.jarch.athena.sharding.ShardingRouter;
import org.apache.commons.lang3.StringUtils;
import org.testng.annotations.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class PGSqlPatternParse {
    private static String inpath = "curd_sql_rollback.txt";
    private static String outpath = "curd_sql_pattern.txt";

    /**
     * 从指定文件中读取数据
     *
     * @param filePath 文件路径
     * @param consumer 每一行的处理函数
     */
    private static void read(String filePath, Consumer<String> consumer) {
        if (StringUtils.isEmpty(filePath)) {
            System.err.println("file:" + filePath + " is illegal!");
            return;
        }
        File file = new File(filePath);
        if (!file.exists()) {
            System.err.println("file:" + filePath + " not exist!");
            return;
        }
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            br.lines().forEach((line) -> consumer.accept(line));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 将数据写入指定文件中
     *
     * @param filePath 文件路径
     * @param data     要写入的数据
     */
    private static void write(String filePath, Supplier<List<String>> data) {
        if (StringUtils.isEmpty(filePath)) {
            System.err.println("file:" + filePath + " is illegal!");
            return;
        }
        tryCreateDirectory(filePath);
        Path path = Paths.get(filePath);
        try {
            Files.write(path, data.get(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建指定文件路径的目录
     *
     * @param filePath 指定文件
     */
    private static void tryCreateDirectory(String filePath) {
        int lastDirectory = filePath.lastIndexOf("/");
        if (lastDirectory <= 0) {
            return;
        }
        Path basePath = Paths.get(filePath.substring(0, lastDirectory + 1));
        if (Files.exists(basePath)) {
            return;
        }
        try {
            Files.createDirectories(basePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test(enabled = false) public void parse() {
        Set<String> patterns = new LinkedHashSet<String>();
        read(inpath, (sql) -> {
            if (StringUtils.isEmpty(sql)) {
                return;
            }
            if (sql.startsWith("BEGIN") || sql.startsWith("ROLLBACK") || sql.startsWith("COMMIT")) {
                return;
            }
            ShardingSQL shardingSQL = ShardingSQL
                .handlePGBasicly(sql, "", ShardingRouter.defaultShardingRouter,
                    new Send2BatchCond(), ShardingSQL.BLACK_FIELDS_FILTER);
            patterns.add(shardingSQL.mostSafeSQL);
        });
        write(outpath, () -> new LinkedList<String>(patterns));
    }
}
