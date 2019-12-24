package com.wangjia.utils;

import com.alibaba.fastjson.JSON;

import java.util.*;


/**
 * Created by Administrator on 2017/4/20.
 */
public final class JavaUtils {

    private static final LinkedList<String> BROWSER_INFOS_KEY = new LinkedList();
    private static final LinkedList<String> BROWSER_INFOS_VALUE = new LinkedList<>();

    static {
        BROWSER_INFOS_KEY.add("Baiduspider");
        BROWSER_INFOS_VALUE.add("百度爬虫");
        BROWSER_INFOS_KEY.add("UCBrowser");
        BROWSER_INFOS_VALUE.add("UC浏览器");
        BROWSER_INFOS_KEY.add("Firefox");
        BROWSER_INFOS_VALUE.add("火狐浏览器");
        BROWSER_INFOS_KEY.add("LBBROWSER");
        BROWSER_INFOS_VALUE.add("猎豹");
        BROWSER_INFOS_KEY.add("MQQBrowser");
        BROWSER_INFOS_VALUE.add("QQ手机");
        BROWSER_INFOS_KEY.add("QQBrowser");
        BROWSER_INFOS_VALUE.add("QQ");
        BROWSER_INFOS_KEY.add("QQ");
        BROWSER_INFOS_VALUE.add("QQ应用");
        BROWSER_INFOS_KEY.add("360Spider");
        BROWSER_INFOS_VALUE.add("360爬虫");
        BROWSER_INFOS_KEY.add("Googlebot");
        BROWSER_INFOS_VALUE.add("google爬虫");
        BROWSER_INFOS_KEY.add("baiduboxapp");
        BROWSER_INFOS_VALUE.add("手机百度");
        BROWSER_INFOS_KEY.add("BingPreview");
        BROWSER_INFOS_VALUE.add("BING浏览器");
        BROWSER_INFOS_KEY.add("OppoBrowser");
        BROWSER_INFOS_VALUE.add("OPPO浏览器");
        BROWSER_INFOS_KEY.add("MicroMessenger");
        BROWSER_INFOS_VALUE.add("微信内置");
        BROWSER_INFOS_KEY.add("VivoBrowser");
        BROWSER_INFOS_VALUE.add("VIVO浏览器");
        BROWSER_INFOS_KEY.add("MiuiBrowser");
        BROWSER_INFOS_VALUE.add("小米浏览器");
        BROWSER_INFOS_KEY.add("huaxiabank");
        BROWSER_INFOS_VALUE.add("华夏银行");
        BROWSER_INFOS_KEY.add("MetaSr");
        BROWSER_INFOS_VALUE.add("搜狗浏览器");
        BROWSER_INFOS_KEY.add("Edge");
        BROWSER_INFOS_VALUE.add("EDGE");
        BROWSER_INFOS_KEY.add("BIDUBrowser");
        BROWSER_INFOS_VALUE.add("百度浏览器");
        BROWSER_INFOS_KEY.add("TaoBrowser");
        BROWSER_INFOS_VALUE.add("淘宝浏览器");
        BROWSER_INFOS_KEY.add("MSIE 5");
        BROWSER_INFOS_VALUE.add("IE5");
        BROWSER_INFOS_KEY.add("MSIE 6");
        BROWSER_INFOS_VALUE.add("IE6");
        BROWSER_INFOS_KEY.add("MSIE 7");
        BROWSER_INFOS_VALUE.add("IE7");
        BROWSER_INFOS_KEY.add("MSIE 8");
        BROWSER_INFOS_VALUE.add("IE8");
        BROWSER_INFOS_KEY.add("MSIE 9");
        BROWSER_INFOS_VALUE.add("IE9");
        BROWSER_INFOS_KEY.add("MSIE 10");
        BROWSER_INFOS_VALUE.add("IE10");
        BROWSER_INFOS_KEY.add("MSIE 11");
        BROWSER_INFOS_VALUE.add("IE11");
        BROWSER_INFOS_KEY.add("rv:1");
        BROWSER_INFOS_VALUE.add("IE11");
        BROWSER_INFOS_KEY.add("MSIE 12");
        BROWSER_INFOS_VALUE.add("IE12");
        BROWSER_INFOS_KEY.add("MSIE 13");
        BROWSER_INFOS_VALUE.add("IE13");
        BROWSER_INFOS_KEY.add("MSIE 14");
        BROWSER_INFOS_VALUE.add("IE14");
        BROWSER_INFOS_KEY.add("MSIE");
        BROWSER_INFOS_VALUE.add("IE");
        BROWSER_INFOS_KEY.add("Chrome");
        BROWSER_INFOS_VALUE.add("Chrome");
        BROWSER_INFOS_KEY.add("Safari");
        BROWSER_INFOS_VALUE.add("Safari");
        BROWSER_INFOS_KEY.add("Other");
        BROWSER_INFOS_VALUE.add("Other");
    }


    /**
     * 通过浏览器Angent信息拿到 浏览器型号
     *
     * @param agent 浏览器Agent信息
     * @return 浏览器信息
     */
    public static String parseBrowserInfo(String agent) {
        for (String key : BROWSER_INFOS_KEY) {
            if (agent.contains(key))
                return key;
        }
        return BROWSER_INFOS_KEY.getLast();
    }

    /**
     * 通过浏览器Angent信息拿到 系统型号
     *
     * @param agent 浏览器Agent信息
     * @return 系统型号
     */
    public static String parseSystemInfo(String agent) {
        String str = agent.toLowerCase();
        //ipad
        if (str.indexOf("ipad") != -1) {
            return "ipad";
        }
        //iphone
        if (str.indexOf("iphone") != -1) {
            return "iphone";
        }
        //android
        if (str.indexOf("android") != -1) {
            return "android";
        }
        //win
        if (str.indexOf("windows") != -1 || str.indexOf("wow") != -1 || str.indexOf("win") != -1) {
            return "windows";
        }
        //mac
        if (str.indexOf("macintosh") != -1 || str.indexOf("mac") != -1 || str.indexOf("ox") != -1) {
            return "mac";
        }
        return "other";
    }

    public static String stringByEndChar(byte[] b, int startIndex, char endChar) {
        int i = startIndex + 1;
        int max = b.length;
        while (i < max) {
            if (b[i] == endChar) {
                return new String(b, startIndex, i - startIndex);
            }
            i += 1;
        }
        return null;
    }

    public static long ip2Long(String ip) {
        long ipNum = 0L;
        try {
            String[] fragments = ip.split("\\Q.\\E");
            if (fragments.length < 4)
                return 0;
            int i = 0;
            while (i < fragments.length) {
                ipNum = Long.parseLong(fragments[i]) | ipNum << 8L;
                i += 1;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ipNum;
    }

    public static String ip2HexString(long ip) {
        String str = Long.toHexString(ip);
        if (str.length() >= 8)
            return str;
        int size = 8 - str.length();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < size; i++)
            sb.append('0');
        return sb.toString() + str;
    }

    public static String ip2HexString(String ip) {
        long v = ip2Long(ip);
        return ip2HexString(v);
    }

    /**
     * 先转成京8区
     * 时间戳转化成1970-01-01到现在的天数
     *
     * @param timeMillis 时间戳/ms
     * @return
     */
    public static int timeMillis2DayNum(long timeMillis) {
        timeMillis += 28800000;
        //不做整除判断，24:00被归到第二天
        return (int) (timeMillis / 86400000 + 1);
    }


    /**
     * 是否是同一天
     *
     * @param timeMillis1
     * @param timeMillis2
     * @return
     */
    public static boolean isSameDate(long timeMillis1, long timeMillis2) {
        timeMillis1 += 28800000;
        timeMillis2 += 28800000;
        if ((timeMillis1 / 86400000) == (timeMillis2 / 86400000))
            return true;
        return false;
    }

    /**
     * 得到小时
     *
     * @param timeMillis
     * @return
     */
    public static int getHour(long timeMillis) {
        timeMillis += 28800000;
        return (int) (timeMillis % 86400000 / 3600000);
    }


    /**
     * 是否是真实IP
     *
     * @param ip
     * @return
     */
    public static boolean isIP(String ip) {
        try {
            if (ip.length() < 7)
                return false;
            byte[] bs = ip.getBytes();
            for (byte b : bs) {
                if (b == '.' || (b >= '0' && b <= '9'))
                    continue;
                return false;
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }


    public static String utf8mb4ToUtf8(String text) {
        byte[] b_text = text.getBytes();
        boolean bChange = false;
        for (int i = 0; i < b_text.length; i++) {
            if ((b_text[i] & 0xF8) == 0xF0) {
                for (int j = 0; j < 4; j++) {
                    b_text[i + j] = 0x30;
                }
                bChange = true;
                i += 3;
            }
        }
        if (bChange) {
            return new String(b_text);
        }
        return text;
    }

    public static <T> List<T> set2List(Set<T> set) {
        List<T> list = new LinkedList<>();
        if (list.addAll(set)) {
            return list;
        }
        return null;
    }

    public static <T> Set<T> list2Set(List<T> list) {
        Set<T> set = new HashSet<>();
        if (set.addAll(list)) {
            return set;
        }
        return null;
    }


    /**
     * 保留几位小数
     *
     * @param num
     * @param holderFraction
     * @return
     */
    public static double holdNfraction(double num, int holderFraction) {
        double base = Math.pow(10.0, holderFraction);
        return Math.floor(num * base) / base;
    }


    /**
     * 简析JSON
     *
     * @param str
     * @param tClass
     * @param <T>
     * @return
     */
    public static <T> T parseJson(String str, Class<T> tClass) {
        return JSON.parseObject(str, tClass);
    }


    /**
     * 简析输入参数
     *
     * @param args
     * @return
     */
    public static Map<String, String> parseParameter(String[] args) {
        HashMap<String, String> map = new HashMap<>();
        String key = null;
        String value = null;
        for (int i = 0; i < args.length; i++) {
            value = args[i];
            if (value.startsWith("--")) {
                key = value.substring(2);
                continue;
            }
            if (key != null) {
                map.put(key, value);
                key = null;
            }
        }
        return map;
    }


    /**
     * 得到URL参数
     *
     * @param url
     * @return
     */
    public static Map<String, String> urlRequest(String url) {
        Map<String, String> mapRequest = new HashMap<String, String>();
        //取参数部分
        String[] arrSplit = null;
        String strUrlParam = null;
        String strURL = url.trim();
        arrSplit = strURL.split("[?]");
        if (strURL.length() > 1) {
            if (arrSplit.length > 1) {
                if (arrSplit[1] != null) {
                    strUrlParam = arrSplit[1];
                }
            }
        }
        if (strUrlParam == null) {
            return mapRequest;
        }
        //简析参数
        arrSplit = strUrlParam.split("[&]");
        for (String strSplit : arrSplit) {
            String[] arrSplitEqual = null;
            arrSplitEqual = strSplit.split("[=]");
            if (arrSplitEqual.length > 1) {
                mapRequest.put(arrSplitEqual[0], arrSplitEqual[1]);
            } else {
                if (arrSplitEqual[0] != "") {
                    mapRequest.put(arrSplitEqual[0], "");
                }
            }
        }
        return mapRequest;
    }


    /**
     * 判断是否乱码
     *
     * @param str
     * @return
     */
    public static boolean isMessyCode(String str) {
        int count = 0;
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if ((int) c == 0xfffd) {
                return true;
            }
            if (c == '%') {
                count++;
            }
        }
        if (count > 5)
            return true;
        return false;
    }
}

