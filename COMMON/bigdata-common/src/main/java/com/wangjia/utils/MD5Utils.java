package com.wangjia.utils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by Administrator on 2017/5/11.
 */
public final class MD5Utils {

    private static final String KEY_MD5 = "MD5";

    public static String MD5(String src) {
        byte[] input = src.getBytes();
        return MD5(input);
    }

    public static String MD5(byte[] input) {
        try {
            MessageDigest md = MessageDigest.getInstance(KEY_MD5);
            md.update(input);
            input = md.digest();
            int length = input.length;
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < length; i++) {
                int val = ((int) input[i]) & 0xff;
                if (val < 16) {
                    sb.append("0");
                }
                sb.append(Integer.toHexString(val));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return null;
    }
}
