package me.ele.jarch.athena.util;

import java.util.Arrays;
import java.util.List;

/**
 * Created by zhengchao on 16/7/7.
 */
public class CyclicList<T> {
    private T[] container;
    private int pointer = 0;
    private final int capacity;

    @SuppressWarnings("unchecked") public CyclicList(int capacity) {
        container = (T[]) new Object[capacity];
        this.capacity = capacity;
    }

    synchronized public T add(T e) {
        T oldEle = container[pointer];
        container[pointer++] = e;
        if (pointer >= capacity) {
            pointer = 0;
        }

        return oldEle;
    }

    @SuppressWarnings("unchecked") synchronized public List<T> takeAll() {
        List<T> oldList;
        if (container[capacity - 1] == null) {
            oldList = Arrays.asList(Arrays.copyOfRange(container, 0, pointer));
        } else {
            oldList = Arrays.asList(container);
        }
        return oldList;
    }
}
