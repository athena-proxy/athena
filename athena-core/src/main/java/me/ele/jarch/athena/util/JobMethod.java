package me.ele.jarch.athena.util;

/**
 * Created by donxuu on 8/4/15.
 */
@FunctionalInterface public interface JobMethod {
    void invoke() throws Exception;
}
