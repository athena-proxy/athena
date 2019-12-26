package me.ele.jarch.athena.pg.proto;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Created by jinghao.wang on 16/11/24.
 */
public class AuthenticationOk extends Authentication {
    public static final AuthenticationOk INSTANCE = new AuthenticationOk();

    @Override protected List<byte[]> getPayload() {
        return Collections.singletonList(PGProto.buildInt32BE(authType.getAuthTypeNum()));
    }

    @Override protected byte getTypeByte() {
        return PGFlags.AUTHENTICATION_OK;
    }

    AuthenticationOk() {
        super(AuthType.AuthenticationOk);
    }

    public static AuthenticationOk loadFromPacket(byte[] packet) {
        PGProto proto = new PGProto(packet, 5);
        int authType = proto.readInt32();
        if (authType != AuthType.AuthenticationOk.getAuthTypeNum()) {
            throw new IllegalArgumentException(
                "illegal auth type byte " + authType + " in packet " + Arrays.toString(packet));
        }
        return INSTANCE;
    }

    @Override public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("AuthenticationOk {}");
        return builder.toString();
    }

}
