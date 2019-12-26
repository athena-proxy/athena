package me.ele.jarch.athena.pg.proto;

import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.pg.util.Severity;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by jinghao.wang on 16/11/24.
 */
public class ErrorResponse extends PGMessage {
    private Severity severity = Severity.ERROR;
    private String sqlState = "";
    private String message = "";
    private String file = "dal_pg.c";
    private String line = "1";
    private String routine = "dal_default_routine";

    private Map<Byte, String> fieldWithValue = new LinkedHashMap<>();

    @Override protected List<byte[]> getPayload() {
        List<byte[]> payload = new LinkedList<>();
        fieldWithValue.forEach((type, value) -> {
            payload.add(new byte[] {type});
            payload.add(PGProto.buildNullStr(value));
        });
        payload.add(new byte[] {0x00});
        return payload;
    }

    @Override protected byte getTypeByte() {
        return PGFlags.ERROR_RESPONSE;
    }

    public ErrorResponse(Severity severity, String sqlState, String message, String file,
        String line, String routine) {
        this.severity = severity;
        fieldWithValue.put(PGFlags.SEVERITY, severity.name());
        this.sqlState = sqlState;
        fieldWithValue.put(PGFlags.CODE, sqlState);
        this.message = message;
        fieldWithValue.put(PGFlags.MESSAGE, message);
        this.file = file;
        fieldWithValue.put(PGFlags.FILE, file);
        this.line = line;
        fieldWithValue.put(PGFlags.LINE, line);
        this.routine = routine;
        fieldWithValue.put(PGFlags.ROUTINE, routine);
    }

    public ErrorResponse(Severity severity, String sqlState, String message) {
        this(severity, sqlState, message, "dal_pg.c", "1", "dal_default_routine");
    }

    public ErrorResponse(String sqlState, String message) {
        this(Severity.ERROR, sqlState, message);
    }

    private ErrorResponse(Map<Byte, String> fieldWithValue) {
        this.fieldWithValue = fieldWithValue;
        update();
    }

    private void update() {
        this.fieldWithValue.forEach((field, value) -> {
            if (PGFlags.SEVERITY == field) {
                severity = Severity.valueOf(value);
            }
            if (PGFlags.CODE == field) {
                sqlState = value;
            }
            if (PGFlags.MESSAGE == field) {
                message = value;
            }
            if (PGFlags.FILE == field) {
                file = value;
            }
            if (PGFlags.LINE == field) {
                line = value;
            }
            if (PGFlags.ROUTINE == field) {
                routine = value;
            }
        });
    }

    /**
     * 该接口只用于添加构造函数参数之外的字段
     *
     * @param field
     * @param value
     */
    public void addField(byte field, String value) {
        fieldWithValue.put(field, value);
    }

    public void addField(Map<Byte, String> fieldWithValue) {
        this.fieldWithValue.putAll(fieldWithValue);
    }

    public Severity getSeverity() {
        return severity;
    }

    public String getSqlState() {
        return sqlState;
    }

    public String getMessage() {
        return message;
    }

    public String getFile() {
        return file;
    }

    public String getLine() {
        return line;
    }

    public Map<Byte, String> getFieldWithValue() {
        return fieldWithValue;
    }

    public static ErrorResponse loadFromPacket(byte[] packet) {
        PGProto proto = new PGProto(packet, 5);
        Map<Byte, String> fieldWithValue = new LinkedHashMap<>();
        while (proto.hasRemaining()) {
            byte field = proto.readByte();
            if (field != 0x00) {
                fieldWithValue.put(field, proto.readNullStr());
            }
        }
        return new ErrorResponse(fieldWithValue);
    }

    /**
     * build DAL custom ErrorResponse
     * message with prefix: [DAL]
     *
     * @param severity
     * @param sqlState
     * @param message
     * @param file
     * @param line
     * @param routine
     * @return
     */
    public static ErrorResponse buildErrorResponse(Severity severity, String sqlState,
        String message, String file, String line, String routine) {
        return new ErrorResponse(severity, sqlState, Constants.DAL + message, file, line, routine);
    }

    public static ErrorResponse buildErrorResponse(Severity severity, String sqlState,
        String message) {
        return buildErrorResponse(severity, sqlState, message, "dal_pg.c", "1",
            "dal_default_routine");
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        ErrorResponse that = (ErrorResponse) o;

        if (severity != that.severity)
            return false;
        if (!sqlState.equals(that.sqlState))
            return false;
        if (!message.equals(that.message))
            return false;
        if (!file.equals(that.file))
            return false;
        if (!line.equals(that.line))
            return false;
        if (!routine.equals(that.routine))
            return false;
        return fieldWithValue.equals(that.fieldWithValue);

    }

    @Override public int hashCode() {
        int result = severity.hashCode();
        result = 31 * result + sqlState.hashCode();
        result = 31 * result + message.hashCode();
        result = 31 * result + file.hashCode();
        result = 31 * result + line.hashCode();
        result = 31 * result + routine.hashCode();
        result = 31 * result + fieldWithValue.hashCode();
        return result;
    }

    @Override public String toString() {
        final StringBuilder sb = new StringBuilder("ErrorResponse{");
        sb.append("severity=").append(severity);
        sb.append(", sqlState='").append(sqlState).append('\'');
        sb.append(", message='").append(message).append('\'');
        sb.append(", file='").append(file).append('\'');
        sb.append(", line='").append(line).append('\'');
        sb.append(", routine='").append(routine).append('\'');
        sb.append(", fieldWithValue=").append(fieldWithValue);
        sb.append('}');
        return sb.toString();
    }
}
