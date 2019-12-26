package me.ele.jarch.athena.util.curator;

import me.ele.jarch.athena.constant.Metrics;
import me.ele.jarch.athena.constant.TraceNames;
import me.ele.jarch.athena.util.etrace.MetricFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;

import java.util.concurrent.CountDownLatch;

/**
 * 当所有缓存都被初始化后，此listener 会收到 INITIALIZED 的事件，此时认为zk初始化结束。latch被释放
 */
public class CacheInitlizedListener implements TreeCacheListener {
    private final CountDownLatch initLatch;

    public CacheInitlizedListener(CountDownLatch initLatch) {
        this.initLatch = initLatch;
    }


    @Override public void childEvent(CuratorFramework client, TreeCacheEvent event)
        throws Exception {
        switch (event.getType()) {
            case INITIALIZED:
                initLatch.countDown();
                MetricFactory.newCounter(Metrics.ZK_CACHE).addTag(TraceNames.STATUS, "FINISH_INIT")
                    .once();
                break;
            default:
                break;
        }
    }
}
