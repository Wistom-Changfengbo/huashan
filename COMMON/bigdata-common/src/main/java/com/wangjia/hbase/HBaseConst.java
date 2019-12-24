package com.wangjia.hbase;

/**
 * Created by Administrator on 2017/4/19.
 */
public final class HBaseConst {
    public static final byte[] BYTES_CF1 = toBytes("cf1");
    public static final byte[] BYTES_CF2 = toBytes("cf2");
    public static final byte[] BYTES_MSG = toBytes("msg");
    public static final byte[] BYTES_N = toBytes("n");
    public static final byte[] BYTES_V = toBytes("v");

    public static final byte[] BYTES_COOKIE = toBytes("cookie");
    public static final byte[] BYTES_IOS_IDFA = toBytes("ios_idfa");
    public static final byte[] BYTES_ANDROID_IMEI = toBytes("android_imei");
    public static final byte[] BYTES_ANDROID_SYSTEMID = toBytes("android_systemid");
    public static final byte[] BYTES_ANDROID_IMEI_SYSTEMID = toBytes("android_imei_systemid");

    public static final byte[] BYTES_PLATFORM = toBytes("platform");
    public static final byte[] BYTES_FIRSTTIME = toBytes("firstTime");
    public static final byte[] BYTES_LASTTIME = toBytes("lastTime");
    public static final byte[] BYTES_APPID = toBytes("appid");
    public static final byte[] BYTES_USERID = toBytes("userid");
    public static final byte[] BYTES_ACCOUNTID = toBytes("accountid");

    public static final byte[] BYTES_TYPE = toBytes("type");
    public static final byte[] BYTES_DTYPE = toBytes("dType");
    public static final byte[] BYTES_DEVICEID = toBytes("deviceid");
    public static final byte[] BYTES_NAME = toBytes("name");
    public static final byte[] BYTES_SYSTEM = toBytes("system");
    public static final byte[] BYTES_SW = toBytes("sw");
    public static final byte[] BYTES_SH = toBytes("sh");
    public static final byte[] BYTES_BCOOKIE = toBytes("bCookie");
    public static final byte[] BYTES_BFLASH = toBytes("bFlash");
    public static final byte[] BYTES_IMEI = toBytes("imei");
    public static final byte[] BYTES_SYSTEMID = toBytes("systemid");
    public static final byte[] BYTES_UUID = toBytes("uuid");
    public static final byte[] BYTES_IP = toBytes("ip");
    public static final byte[] BYTES_REF = toBytes("ref");
    public static final byte[] BYTES_ADDRESS = toBytes("address");
    public static final byte[] BYTES_START = toBytes("start");
    public static final byte[] BYTES_TIME = toBytes("time");
    public static final byte[] BYTES_NUM = toBytes("num");
    public static final byte[] BYTES_ENUM = toBytes("eNum");
    public static final byte[] BYTES_VERSION = toBytes("version");
    public static final byte[] BYTES_NET = toBytes("net");
    public static final byte[] BYTES_EVENTIDS = toBytes("eventIds");
    public static final byte[] BYTES_UNIONID = toBytes("unionid");
    public static final byte[] BYTES_GPS = toBytes("gps");
    public static final byte[] BYTES_ACC = toBytes("acc");

    public static final byte[] BYTES_URL = toBytes("url");
    public static final byte[] BYTES_UCID = toBytes("ucid");
    public static final byte[] BYTES_PHONE = toBytes("phone");
    public static final byte[] BYTES_MAC = toBytes("mac");
    public static final byte[] BYTES_WEIGHT = toBytes("weight");

    //IM
    public static final byte[] BYTES_IM_CUSTOMER = toBytes("customer");
    public static final byte[] BYTES_IM_TYPE = toBytes("type");
    public static final byte[] BYTES_IM_FROM_TYPE = toBytes("from_type");
    public static final byte[] BYTES_IM_TIME = toBytes("time");
    public static final byte[] BYTES_IM_CONTENT = toBytes("content");
    public static final byte[] BYTES_IM_CLIENT_IP = toBytes("client_ip");
    public static final byte[] BYTES_IM_CLIENT_ID = toBytes("client_id");
    public static final byte[] BYTES_IM_CLIENT = toBytes("client");
    public static final byte[] BYTES_IM_SOURCE = toBytes("source");
    public static final byte[] BYTES_IM_AVATAR = toBytes("avatar");

    public static final String NULL_STRING_VALUE = "null";

    private static final byte[] toBytes(String str) {
        return str.getBytes();
    }
}
