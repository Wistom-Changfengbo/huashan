package com.wangjia.hbase;

/**
 * Created by Administrator on 2017/4/24.
 */
public final class HBaseTableName {
    /**
     * 老版表名前缀
     */
    public static final String TB_HEAD_OLD = "v7";
    /**
     * 新版表名前缀
     */
    public static final String TB_HEAD_NEW = "v9";

    private static String tbName(String name) {
        return TB_HEAD_NEW + name;
    }

    private static String tbName(String name,String head) {
        return head + name;
    }

    @Deprecated
    public static final String COOKIE_UUID              = tbName("cookie2uuid");
    @Deprecated
    public static final String IOS_IDFA_UUID            = tbName("iosIdfa2uuid");
    @Deprecated
    public static final String ANDROID_IMEI_UUID        = tbName("androidImei2uuid");
    @Deprecated
    public static final String ANDROID_SYSTEMID_UUID    = tbName("androidSystemId2uuid");
    @Deprecated
    public static final String MAC_UUID                 = tbName("mac2uuid");
    @Deprecated
    public static final String PHONE_UUID               = tbName("phone2uuid");
    @Deprecated
    public static final String UUID_DEVICEID            = tbName("uuid2deviceId");
    @Deprecated
    public static final String USERID_UUID              = tbName("userid2uuid");
    @Deprecated
    public static final String IP_UUID                  = tbName("ip2uuid");
    @Deprecated
    public static final String UUID_LABEL_DETAILS       = tbName("uuid2labelDetails");
    @Deprecated
    public static final String UUID_LABEL               = tbName("uuid2label");
    @Deprecated
    public static final String USERCENTER               = tbName("usercenter");
    @Deprecated
    public static final String URL_UUID                 = tbName("url2uuid");


    public static final String EVENT_DES                = tbName("eventdes");
    public static final String MAC_PROBE                = tbName("mac2probe");
    public static final String MAC_PROBE_VISIT          = tbName("mac2probevisit");
    public static final String URL_BROWSE_LOG           = tbName("url2browselog");
    public static final String UUID_APPLIST             = tbName("uuid2applist");
    public static final String UUID_DEVICE_MSG          = tbName("uuid2devicemsg");
    public static final String UUID_DEVICE_GPS          = tbName("uuid2devicegps");
    public static final String UUID_ITEM_STAR           = tbName("uuid2itemstar");
    public static final String UUID_LABEL_APPLIST       = tbName("uuid2labelapplist");
    public static final String UUID_LABEL_DES           = tbName("uuid2labeldes");
    public static final String UUID_LABEL_GPS           = tbName("uuid2labelgps");
    public static final String UUID_LABEL_GUIDE         = tbName("uuid2labelguide");
    public static final String UUID_LABEL_SUM           = tbName("uuid2labelsum");
    public static final String UUID_VISIT               = tbName("uuid2visit");
    public static final String UUID_VISIT_DES           = tbName("uuid2visitdes");
    public static final String UUID_HABIT               = tbName("uuid2habit");
    public static final String IM_CHAT_MSG              = tbName("imchatmsg");

    public static final String UUID_USER_TAG            = tbName("uuid2usertag");
    public static final String UUID_USER_IMAGE          = tbName("uuid2userimage");
    public static final String EQID_KEYWORD             = tbName("eqid2keyword");

    public static final String RECOMMEND_PHOTO_CASE     = tbName("recommend_photo_case");
    public static final String RECOMMEND_PUSH           = tbName("recommend_push");
}
