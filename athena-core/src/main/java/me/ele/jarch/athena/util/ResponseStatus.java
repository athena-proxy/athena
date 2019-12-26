package me.ele.jarch.athena.util;

import com.github.mpjct.jmpjct.util.ErrorCode;

/**
 * Created by jinghao.wang on 16/1/11.
 */
public final class ResponseStatus {
    public enum ResponseType {
        OK, DBERR, DALERR, CROSS_EZONE, ABORT
    }


    private ResponseType responseType;
    private ErrorCode code;
    private String message = "";

    public ResponseStatus(ResponseType responseType) {
        this.responseType = responseType;
    }

    public ResponseStatus(ResponseType responseType, ErrorCode code, String message) {
        this.responseType = responseType;
        this.code = code;
        this.message = message;
    }

    public ResponseStatus setCode(ErrorCode code) {
        this.code = code;
        return this;
    }

    public ResponseStatus setMessage(String m) {
        this.message = m;
        return this;
    }

    public ResponseType getResponseType() {
        return responseType;
    }

    public ErrorCode getCodeOrDefaultDALERR() {
        return getCodeOrDefault(ErrorCode.ERR);
    }

    public ErrorCode getCodeOrDefault(ErrorCode defaultCode) {
        return code == null ? defaultCode : code;
    }

    public String getMessage() {
        return message;
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        ResponseStatus that = (ResponseStatus) o;

        if (responseType != that.responseType)
            return false;
        if (code != that.code)
            return false;
        return message.equals(that.message);

    }

    @Override public int hashCode() {
        int result = responseType.hashCode();
        result = 31 * result + (code != null ? code.hashCode() : 0);
        result = 31 * result + message.hashCode();
        return result;
    }

    @Override public String toString() {
        StringBuilder sb = new StringBuilder(64);
        print(sb);
        return sb.toString();
    }

    public void print(StringBuilder stringBuilder) {
        stringBuilder.append(responseType.toString()).append(":(")
            .append((code == null ? 0 : code.getIdentifier())).append("|").append(message)
            .append(")");
    }
}
