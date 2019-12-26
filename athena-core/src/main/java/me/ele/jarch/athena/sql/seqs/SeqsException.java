package me.ele.jarch.athena.sql.seqs;

/**
 * Created by xczhang on 15/8/19 上午11:33.
 */
public class SeqsException extends Exception {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    public SeqsException(String message) {
        super(message);
    }

    public SeqsException(String message, Throwable cause) {
        super(message, cause);
    }

    public SeqsException(Throwable cause) {
        super(cause);
    }
}
