package me.ele.jarch.athena.util.log;

import java.util.StringJoiner;

/**
 * @author jinghao.wang
 */
public class RawSQLContxt {
    private String rawSQL;
    private String sqlWithoutComment;
    private String clientInfo;
    private String user;
    private String dalGroup;
    private String transactionId;

    public RawSQLContxt() {
    }

    public String rawSQL() {
        return rawSQL;
    }

    public RawSQLContxt rawSQL(String rawSQL) {
        this.rawSQL = rawSQL;
        return this;
    }

    public String sqlWithoutComment() {
        return sqlWithoutComment;
    }

    public RawSQLContxt sqlWithoutComment(String sqlWithoutComment) {
        this.sqlWithoutComment = sqlWithoutComment;
        return this;
    }

    public String clientInfo() {
        return clientInfo;
    }

    public RawSQLContxt clientInfo(String clientInfo) {
        this.clientInfo = clientInfo;
        return this;
    }

    public String transactionId() {
        return transactionId;
    }

    public RawSQLContxt transactionId(String transactionId) {
        this.transactionId = transactionId;
        return this;
    }

    public String user() {
        return user;
    }

    public RawSQLContxt user(String user) {
        this.user = user;
        return this;
    }

    public String dalGroup() {
        return dalGroup;
    }

    public RawSQLContxt dalGroup(String dalGroup) {
        this.dalGroup = dalGroup;
        return this;
    }

    @Override public String toString() {
        return new StringJoiner(", ", RawSQLContxt.class.getSimpleName() + "[", "]")
            .add("rawSQL='" + rawSQL + "'").add("sqlWithoutComment='" + sqlWithoutComment + "'")
            .add("clientInfo='" + clientInfo + "'").add("user='" + user + "'")
            .add("dalGroup='" + dalGroup + "'").add("transactionId='" + transactionId + "'")
            .toString();
    }
}
