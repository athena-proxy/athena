package me.ele.jarch.athena.scheduler;

import java.util.Arrays;
import java.util.function.IntPredicate;

/**
 * 可记录一定数量历史数据的计数器
 * Created by jinghao.wang on 16/4/7.
 * #not thread-safe#
 */
class WindowCounter {
    private int lastIndex = 0;
    private int[] history;

    public WindowCounter(int capacity) {
        history = new int[capacity];
    }

    public int getLastIndex() {
        return lastIndex;
    }

    public void record(int snapshot) {
        history[lastIndex++ % history.length] = snapshot;
    }

    public int getHistory() {
        return Arrays.stream(history).sum();
    }

    public boolean allMatch(IntPredicate predicate) {
        return Arrays.stream(history).allMatch(predicate);
    }
}
