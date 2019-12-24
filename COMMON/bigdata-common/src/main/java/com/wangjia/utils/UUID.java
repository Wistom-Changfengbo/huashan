package com.wangjia.utils;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

/**
 * Created by Administrator on 2017/4/19.
 */
public final class UUID implements Serializable {
    private char[] base64 = null;
    private String[] blankStr = null;
    private Random random = null;

    private UUID() {
        byte[] bs = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".getBytes();
        base64 = new char[bs.length];
        for (int i = 0; i < bs.length; i++) {
            base64[i] = (char) bs[i];
        }
        blankStr = new String[10];
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            sb.append('A');
            blankStr[i] = sb.toString();
        }
        random = new Random();
    }

    private static UUID instance = new UUID();

    private String long2Base64(long l, int minSize) {
        StringBuilder sb = new StringBuilder();
        while (l != 0) {
            int i = (int) (l % 64);
            sb.append(base64[i]);
            l /= 64;
        }
        int d = minSize - sb.length() - 1;
        if (d < 0)
            return sb.reverse().toString();
        sb.append(blankStr[d]);
        return sb.reverse().toString();
    }

    private String long2Base64(long l) {
        return long2Base64(l, 8);
    }

    private String makeUUID() {
        long time = System.currentTimeMillis();
        String tStr = long2Base64(time);
        long rand = random.nextLong();
        if (rand < 0)
            rand = (-rand) % 280000000000000L;
        else
            rand = rand % 280000000000000L;
        String rStr = long2Base64(rand);
        return rStr + tStr;
    }

    public static String randomUUID() {
        return instance.makeUUID();
    }

    public static List<String> randomUUID(int size) {
        String uuid = null;
        List<String> list = new LinkedList<String>();
        for (int i = 0; i < size; i++) {
            uuid = UUID.randomUUID();
            list.add(uuid);
        }
        return list;
    }
}
