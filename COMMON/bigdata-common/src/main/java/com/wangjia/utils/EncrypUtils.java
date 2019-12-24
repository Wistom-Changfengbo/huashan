package com.wangjia.utils;

import java.net.URLDecoder;
import java.util.Base64;

/**
 * Created by Administrator on 2018/3/6.
 */
public class EncrypUtils {

    public static String encryption(String source, String key) {
        final byte[] bKey = key.getBytes();
        return encryption(source, bKey);
    }

    public static String encryption(String source, final byte[] key) {
        final byte[] bSource = source.getBytes();

        int is = 0;
        int ik = 0;
        final int maxs = bSource.length;
        final int maxk = key.length;

        while (is < maxs) {
            bSource[is] ^= key[ik];
            ik++;
            is++;
            if (ik == maxk)
                ik = 0;
        }
        return new String(Base64.getEncoder().encode(bSource));
    }

    public static String decrypt(String source, String key) {
        final byte[] bKey = key.getBytes();
        return decrypt(source, bKey);
    }

    public static String decrypt(String source, final byte[] key) {
        final byte[] bSource = Base64.getDecoder().decode(source.getBytes());
        int is = 0;
        int ik = 0;
        final int maxs = bSource.length;
        final int maxk = key.length;

        while (is < maxs) {
            bSource[is] ^= key[ik];
            ik++;
            is++;
            if (ik == maxk)
                ik = 0;
        }
        return new String(bSource);
    }

    public static void main(String[] args) throws Exception {

        String testStr = "UEvVFMdLF0mVVsXeWBFWEVbYWF2EmgXVFdFXVFQGkExXiIVDGB5BHAHAg1zcQYGAQN2CnVWJldVUAsDVgUnA3MEdQ8GYG0QL1FUaiomEQsRBnEDdFR+AQcIUQYIAiEOJwJkGxQsJEYdRE5FJmAJE2R9B3BjHGZaQ21dVV9QZw5mVihTRC0oVmAcFVowHUVUQUcoVi8SfhcFHAIaAxdpFjdONUNTLx5cI11SF3lgRl9YWi5OYxxmRklBR1FfajNRNkQvWFhgexA3XlxbLDURHRFDK2Y0WSAXChARGBBNPGs3VDRSUyxjCGABBw1zOgIIAQRjRG0SIUNVXEdHEA8eT2ZTJ0NXYHtJYFhYQDAnbEVKRCRmKFRmDxICERgQRTddJ1IZXlJgexByEhsXMDZKXVZrKF1jCmYECRBOGBBGMFYbQz9HU2B7ECRZW0EmMGxSUkckZi1ZN0ESHhFAW1ggFn4VdwMPenAKdAEDAXFzABMfFjVAMVVmDxIGAxZPaDg%3D";
        System.out.println(testStr);
        testStr = URLDecoder.decode(testStr, "UTF-8");

        System.out.println(testStr);
        //System.out.println("源：" + testStr);
        //String eStr = encryption(testStr, "7F76BA2B075CB3134A9A0D5023425E4D");
        //System.out.println("加密后：" + eStr);
        String sStr = decrypt(testStr, "7F76BA2B075CB3134A9A0D5023425E4D");
        System.out.println("解密后：" + sStr);
    }
}