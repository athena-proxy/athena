package me.ele.jarch.athena.netty.upgrade;

public enum UdsCmd {
    UNKNOWN(""), CLOSE_UPGRADE_UDS("close_upgrade_uds"), CLOSE_LISTEN_PORTS("close_listen_ports");
    public final String cmd;

    private UdsCmd(String cmd) {
        this.cmd = cmd;
    }

    public static UdsCmd getType(String cmd) {
        for (UdsCmd udsCmd : values()) {
            if (udsCmd.cmd.equalsIgnoreCase(cmd)) {
                return udsCmd;
            }
        }
        return UNKNOWN;
    }
}
