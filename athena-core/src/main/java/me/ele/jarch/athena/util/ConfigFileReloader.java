package me.ele.jarch.athena.util;

import java.io.File;
import java.util.function.Consumer;

/**
 * Created by donxuu on 9/18/15.
 * <p>
 * 定期检查一个配置文件,如果更新了就加载这个文件.
 * 1. 服务启动时,如果一个文件存在,立即加载, 因为启动服务时,文件通常是预备好的.
 * 2. 在服务运行中,如果检查到一个文件发生变化,等待一轮, 确保该文件被完全复制后, 在下一轮加载.
 * 3. 启动时,如果一个文件不存在, 则确保按 #2 的方式动态加载.
 */
public class ConfigFileReloader {
    private final String configFile;
    private final Consumer<String> consumer;
    private long lastTimestamp = -1;
    private boolean delay = false;

    public ConfigFileReloader(String configFile, Consumer<String> consumer) {
        this.configFile = configFile;
        this.consumer = consumer;

    }

    public boolean tryLoad() throws InterruptedException {
        File file = new File(this.configFile);
        if (!file.exists()) {
            delay = true;
            // proceed, letting the consumer to report a proper message.
            consumer.accept(this.configFile);
            return false;
        }

        long currentTimestamp = file.lastModified();
        if (lastTimestamp == currentTimestamp) {
            return false;
        }


        if (delay) {
            delay = false;
            return false;
        }

        try {
            consumer.accept(this.configFile);
            lastTimestamp = currentTimestamp;
            return true;
        } finally {
            delay = true;
        }

    }
}
