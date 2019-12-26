package me.ele.jarch.athena.pg.proto;

/**
 * Created by jinghao.wang on 16/11/23.
 */
public abstract class Authentication extends PGMessage {
    protected final AuthType authType;

    public Authentication(AuthType authType) {
        this.authType = authType;
    }

    /**
     * note:
     * 目前支持MD5加密密码的方式登录,所以只实现了MD5和Ok两种认证类型
     *
     * @param packet
     * @return
     */
    public static Authentication loadFromPacket(byte[] packet) {
        PGProto proto = new PGProto(packet, 5);
        AuthType authType = AuthType.getAuthType(proto.readInt32());
        switch (authType) {
            case AuthenticationMD5Password:
                return AuthenticationMD5Password.loadFromPacket(packet);
            case AuthenticationOk:
                return AuthenticationOk.loadFromPacket(packet);
            default:
                throw new IllegalArgumentException("not implement auth method " + authType);
        }
    }
}
