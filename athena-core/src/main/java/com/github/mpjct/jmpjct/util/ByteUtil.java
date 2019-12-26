package com.github.mpjct.jmpjct.util;

public class ByteUtil {

    /**
     * byte array convert to hexString
     *
     * @param byte[]
     * @return HexString
     */
    public static final String bytesToHexString(byte[] bArray) {
        StringBuffer sb = new StringBuffer(bArray.length);
        String sTemp;
        for (int i = 0; i < bArray.length; i++) {
            sTemp = Integer.toHexString(0xFF & bArray[i]);
            if (sTemp.length() < 2)
                sb.append(0);
            sb.append(sTemp.toUpperCase());
        }
        return sb.toString();
    }

    /**
     * hexString convert to byte array
     *
     * @param hexString
     * @return byte[]
     */
    public static byte[] hexStringToByte(String hex) {
        int len = (hex.length() / 2);
        byte[] result = new byte[len];
        char[] achar = hex.toCharArray();
        for (int i = 0; i < len; i++) {
            int pos = i * 2;
            result[i] = (byte) (toByte(achar[pos]) << 4 | toByte(achar[pos + 1]));
        }
        return result;
    }

    private static int toByte(char c) {
        byte b = (byte) "0123456789ABCDEF".indexOf(c);
        return b;
    }

    @SuppressWarnings("unused") public static void main(String args[]) {
        String hexString = "81F5E21E35407D884A6CD4A731AEBFB6AF209E1B";
        byte[] hex = hexStringToByte(hexString);

        byte[] hex1 =
            {98, 14, 37, 104, 78, 47, -32, -57, -4, -85, 100, -50, 29, 58, -63, -99, -105, 101, 21,
                12};
        String hexString1 = bytesToHexString(hex1);
    }

}
