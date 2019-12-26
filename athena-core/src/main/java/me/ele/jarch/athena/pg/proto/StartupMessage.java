package me.ele.jarch.athena.pg.proto;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by jinghao.wang on 16/11/23.
 */
public class StartupMessage extends PGMessage {
    public static final int PROTOCOL_VERSION_NUM = 196608;
    private int protocolVersionNum = PROTOCOL_VERSION_NUM;
    private final String user;
    private final String database;
    private final LinkedHashMap<String, String> parameters;

    @Override protected List<byte[]> getPayload() {
        List<byte[]> payload = new LinkedList<>();
        payload.add(PGProto.buildInt32BE(protocolVersionNum));
        parameters.forEach((name, value) -> {
            payload.add(PGProto.buildNullStr(name));
            payload.add(PGProto.buildNullStr(value));
        });
        payload.add(new byte[] {0});
        return payload;
    }

    @Override protected byte getTypeByte() {
        return PGFlags.C_STARTUP_MESSAGE;
    }

    public StartupMessage(String user, String database) {
        this.user = user;
        this.database = database;
        parameters = new LinkedHashMap<>();
        parameters.put("user", user);
        parameters.put("database", database);
    }

    public StartupMessage(String user) {
        this(user, user);
    }

    public Map<String, String> getArgs() {
        Map<String, String> args = new LinkedHashMap<>(parameters);
        args.remove("user");
        args.remove("database");
        return args;
    }

    public void addArgs(String argName, String argValue) {
        parameters.put(argName, argValue);
    }

    public void addArgsIfAbsent(String argName, String argValue) {
        parameters.putIfAbsent(argName, argValue);
    }

    public String getUser() {
        return user;
    }

    public String getDatabase() {
        return database;
    }

    public void setProtocolVersionNum(int protocolVersionNum) {
        this.protocolVersionNum = protocolVersionNum;
    }

    public static StartupMessage loadFromPacket(byte[] packet) {
        return _loadFromPacket(packet, false);
    }

    public static StartupMessage loadFromVirtualPacket(byte[] packet) {
        return _loadFromPacket(packet, true);
    }

    private static StartupMessage _loadFromPacket(byte[] packet, boolean isVirtual) {
        PGProto proto = new PGProto(packet, isVirtual ? 5 : 4);
        Map<String, String> parameters = new LinkedHashMap<>();
        int protocolVersionNum = proto.readInt32();
        while (proto.hasRemaining()) {
            String argName = proto.readNullStr();
            if (!argName.trim().isEmpty()) {
                parameters.put(argName, proto.readNullStr());
            }
        }
        String user = parameters.get("user");
        String database = parameters.getOrDefault("database", user);
        StartupMessage startupMessage = new StartupMessage(user, database);
        startupMessage.setProtocolVersionNum(protocolVersionNum);
        parameters.forEach((name, value) -> startupMessage.addArgsIfAbsent(name, value));
        return startupMessage;
    }

    @Override public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("StartupMessage {protocolVersionNum=");
        builder.append(protocolVersionNum);
        builder.append(", user=");
        builder.append(user);
        builder.append(", database=");
        builder.append(database);
        builder.append(", parameters=");
        builder.append(parameters);
        builder.append("}");
        return builder.toString();
    }

}
