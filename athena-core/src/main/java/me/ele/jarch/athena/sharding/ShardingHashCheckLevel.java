package me.ele.jarch.athena.sharding;

public enum ShardingHashCheckLevel {
    PASS("pass"), WARN("warn"), REJECT("reject");
    private final String authLevel;

    ShardingHashCheckLevel(String level) {
        this.authLevel = level;
    }

    public String getAuthLevel() {
        return authLevel;
    }

    public static ShardingHashCheckLevel getCheckLevel(String level) {
        switch (level) {
            case "pass":
                return PASS;
            case "reject":
                return REJECT;
            default:
                return WARN;
        }
    }
}
