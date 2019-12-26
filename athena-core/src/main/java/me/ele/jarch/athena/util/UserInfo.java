package me.ele.jarch.athena.util;

/**
 * dal-crendentials.cfg配置文件中每行记录
 * 的实体映射
 * Created by jinghao.wang on 16/6/2.
 */
public class UserInfo {
    /**
     * 配置文件中字面用户名
     * eg:
     * utp_user@ers-bpm_mkt_utp_group
     * 此时 literalName != name
     * 或
     * misc_wizard_group_user
     * 此时 literalName == name
     */
    private final String literalName;
    //解析后的用户名,不包含通道信息
    private final String name;
    private final String encryptedPwd; //gaiming
    //用户关联的通道名,可能为空字符串""
    private final String schema;
    private boolean readOnly = false;

    public UserInfo(String literalName, String encryptedPwd) {
        String[] splitedLiteralName = literalName.split("@");
        this.literalName = literalName;
        this.name = splitedLiteralName[0];
        this.encryptedPwd = encryptedPwd;
        this.schema = splitedLiteralName.length == 2 ? splitedLiteralName[1] : "";
    }

    public UserInfo(String literalName, String encryptedPwd, boolean isReadOnly) {
        this(literalName, encryptedPwd);
        this.readOnly = isReadOnly;
    }

    public String getLiteralName() {
        return literalName;
    }

    public String getName() {
        return name;
    }

    public String getEncryptedPwd() {
        return encryptedPwd;
    }

    public String getSchema() {
        return schema;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    @Override public String toString() {
        return "UserInfo{" + "literalName='" + literalName + '\'' + ", name='" + name + '\''
            + ", encryptedPwd='" + encryptedPwd + '\'' + ", schema='" + schema + '\''
            + ", readOnly=" + readOnly + '}';
    }
}
