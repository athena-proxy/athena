package com.github.mpjct.jmpjct.mysql.proto;

/*
 * A MySQL ERR Packet
 *
 * https://dev.mysql.com/doc/refman/5.5/en/error-messages-server.html
 * https://dev.mysql.com/doc/refman/5.5/en/error-messages-client.html
 *
 */

import me.ele.jarch.athena.constant.Constants;

import java.util.ArrayList;

public class ERR extends Packet {
    private static final byte SQLSTATE_MARKER = (byte) '#';
    public long errorCode = 0;
    public String sqlState = "HY000";
    public String errorMessage = "";

    public ArrayList<byte[]> getPayload() {
        ArrayList<byte[]> payload = new ArrayList<byte[]>();

        payload.add(Proto.build_byte(Flags.ERR));
        payload.add(Proto.build_fixed_int(2, this.errorCode));
        payload.add(Proto.build_byte(SQLSTATE_MARKER));
        payload.add(Proto.build_fixed_str(5, this.sqlState));
        payload.add(Proto.build_fixed_str(this.errorMessage.length(), this.errorMessage));

        return payload;
    }

    public static ERR loadFromPacket(byte[] packet) {
        ERR obj = new ERR();
        Proto proto = new Proto(packet, 3);

        obj.sequenceId = proto.get_fixed_int(1);
        proto.get_filler(1);
        obj.errorCode = proto.get_fixed_int(2);

        if (proto.has_remaining_data()) {
            byte[] marker = proto.get_fixed_bytes(1);
            if (marker[0] == SQLSTATE_MARKER) {
                obj.sqlState = proto.get_fixed_str(5);
            } else {
                proto.get_filler(-1);
            }
            obj.errorMessage = proto.get_eop_str();
        }
        return obj;
    }

    public static ERR buildErr(long sequenceId, long errorCode, String errorMessage) {
        ERR err = new ERR();
        err.sequenceId = sequenceId;
        err.errorCode = errorCode;
        err.errorMessage = Constants.DAL + errorMessage;
        return err;
    }

    public static ERR buildErr(long sequenceId, long errorCode, String sqlState,
        String errorMessage) {
        ERR err = new ERR();
        err.sequenceId = sequenceId;
        err.sqlState = sqlState;
        err.errorCode = errorCode;
        err.errorMessage = Constants.DAL + errorMessage;
        return err;
    }

    public static void main(String[] args) {
        // byte[] packet = Proto.packet_string_to_bytes(
        // "17 00 00 01 ff 48 04 23    48 59 30 30 30 4e 6f 20"
        // + "74 61 62 6c 65 73 20 75    73 65 64"
        // );
        byte[] packet =
            {23, 0, 0, 0, -1, 16, 4, 84, 111, 111, 32, 109, 97, 110, 121, 32, 99, 111, 110, 110,
                101, 99, 116, 105, 111, 110, 115};
        ERR.loadFromPacket(packet);
    }

}
