package me.ele.jarch.athena.allinone;

import me.ele.jarch.athena.exception.QuitException;

import java.util.List;

/**
 * 抽象选择策略接口
 * Created by jinghao.wang on 2018/12/6.
 */
interface SelectStrategy<T> {
    T select(List<T> candidates) throws QuitException;
}