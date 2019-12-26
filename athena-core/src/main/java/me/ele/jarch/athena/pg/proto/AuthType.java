package me.ele.jarch.athena.pg.proto;

/**
 * Created by jinghao.wang on 16/11/24.
 */
public enum AuthType {
    AuthenticationOk(0), AuthenticationKerberosV5(2), AuthenticationCleartextPassword(
        3), AuthenticationMD5Password(5), AuthenticationSCMCredential(6), AuthenticationGSS(
        7), AuthenticationGSSContinue(8), AuthenticationSSPI(9);

    private final int authMethodNum;

    AuthType(int authMethodNum) {
        this.authMethodNum = authMethodNum;
    }

    public int getAuthTypeNum() {
        return authMethodNum;
    }

    public static AuthType getAuthType(int type) {
        switch (type) {
            case 0:
                return AuthenticationOk;
            case 2:
                return AuthenticationKerberosV5;
            case 3:
                return AuthenticationCleartextPassword;
            case 5:
                return AuthenticationMD5Password;
            case 6:
                return AuthenticationSCMCredential;
            case 7:
                return AuthenticationGSS;
            case 8:
                return AuthenticationGSSContinue;
            case 9:
                return AuthenticationSSPI;
            default:
                throw new IllegalArgumentException("illegal auth method type " + type);
        }
    }
}
