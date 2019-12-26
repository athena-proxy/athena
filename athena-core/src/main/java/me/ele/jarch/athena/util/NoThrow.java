package me.ele.jarch.athena.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code NoThrow} calls a function and catch any exception.
 * This Class is intended to be used when destroy and clear resources, at the occasion nothing helps except write to log.
 * Think twice before use {@code NoThrow}
 * Created by dongyan.xu on 11/12/16.
 */

public class NoThrow {
    private static final Logger logger = LoggerFactory.getLogger(NoThrow.class);


    @FunctionalInterface public interface NormalFunc {
        void invoke() throws Throwable;
    }

    /**
     * Invoke the input {@code NormalFunc}, catch Throwable and no throw any longer.
     *
     * @param normalFunc A lambda with neither input nor returning value
     */
    public static void call(NormalFunc normalFunc) {
        try {
            normalFunc.invoke();
        } catch (Throwable t) {  //NOSONAR
            logger.error(t.getMessage(), t);
        }

    }

    @FunctionalInterface public interface ExceptionFunc {
        void invoke(Throwable e);
    }

    /**
     * Invoke the input {@code NormalFunc}, catch Throwable and call {@code ExceptionFunc}
     *
     * @param normalFunc    A lambda to execute normal logic with neither input nor returning value
     * @param exceptionFunc A lambda to execute error logic when exception happens with neither input nor returning value
     */
    public static void execute(NormalFunc normalFunc, ExceptionFunc exceptionFunc) {
        try {
            normalFunc.invoke();
        } catch (Throwable e) { // NOSONAR
            exceptionFunc.invoke(e);
        }
    }
}
