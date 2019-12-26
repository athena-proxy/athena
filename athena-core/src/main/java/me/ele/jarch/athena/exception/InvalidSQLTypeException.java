package me.ele.jarch.athena.exception;

/**
 * Created by zhengchao on 16/8/29.
 */
public class InvalidSQLTypeException extends RuntimeException {
    /**
     *
     */
    private static final long serialVersionUID = 6714041008365913886L;

    public InvalidSQLTypeException(String msg) {
        super(msg);
    }
}
