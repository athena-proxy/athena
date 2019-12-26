package me.ele.jarch.athena.pg.util;

/**
 * Created by jinghao.wang on 16/11/26.
 */
public enum Severity {
    /**
     * In an error message
     */
    ERROR, FATAL, PANIC,
    /**
     * In a notice message
     */
    WARNING, NOTICE, DEBUG, INFO, LOG
}
