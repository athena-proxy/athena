package me.ele.jarch.athena.exception;

import com.github.mpjct.jmpjct.mysql.proto.ERR;
import com.github.mpjct.jmpjct.util.ErrorCode;

public class QueryException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public final ErrorCode errorCode;
    public final long sequenceId;
    public final String sqlState;
    public final String errorMessage;

    private QueryException(Builder builder, Throwable t) {
        super(builder.errorMessage, t);
        this.sequenceId = builder.sequenceId;
        this.sqlState = builder.sqlState;
        this.errorCode = builder.errorCode;
        this.errorMessage = builder.errorMessage;
    }

    private QueryException(Builder builder) {
        super(builder.errorMessage);
        this.sequenceId = builder.sequenceId;
        this.sqlState = builder.sqlState;
        this.errorCode = builder.errorCode;
        this.errorMessage = builder.errorMessage;
    }

    public static class Builder {
        // Required parameters
        private final ErrorCode errorCode;

        // Optional parameters - initialized to default values
        private long sequenceId = 1;
        private String sqlState = "HY000";
        private String errorMessage = "";

        public Builder(ErrorCode errorCode) {
            this.errorCode = errorCode;
        }

        public Builder setSequenceId(long sequenceId) {
            this.sequenceId = sequenceId;
            return this;
        }

        public Builder setSqlState(String sqlState) {
            this.sqlState = sqlState;
            return this;
        }

        public Builder setErrorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
            return this;
        }

        public QueryException bulid(Throwable t) {
            return new QueryException(this, t);
        }

        public QueryException bulid() {
            return new QueryException(this);
        }
    }

    public ERR loadERRFrom() {
        return ERR.buildErr(sequenceId, errorCode.getErrorNo(), errorMessage);
    }

    @Override public String toString() {
        return "ErrorCode:" + errorCode + ",ErrorNo:" + errorCode.getErrorNo() + ",ErrorMessage:"
            + errorMessage + ",sequenceId:" + sequenceId;
    }
}
