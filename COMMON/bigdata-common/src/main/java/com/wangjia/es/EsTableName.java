package com.wangjia.es;

/**
 * Created by Administrator on 2017/9/28.
 */
public final class EsTableName {
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

    public static final String ES_INDEX_BIGDATA_VISIT = tbName("bigdata-visit");
    public static final String ES_TYPE_VISIT = tbName("visit");

    public static final String ES_INDEX_BIGDATA_DEVICE = tbName("bigdata-device");
    public static final String ES_TYPE_DEVICE = tbName("device");

    public static final String ES_INDEX_BIGDATA_APP = tbName("bigdata-app");
    public static final String ES_TYPE_APPLIST = tbName("applist");

    public static final String ES_INDEX_BIGDATA_APP_LABEL = tbName("bigdata-app-labal");
    public static final String ES_TYPE_APP_LABEL = tbName("applabel");

    public static final String ES_INDEX_BIGDATA_APP_IMAGE = tbName("bigdata-user-image");
    public static final String ES_TYPE_APP_IMAGE = tbName("user-image");

    public static final String ES_INDEX_BIGDATA_IM = tbName("bigdata-im");
    public static final String ES_TYPE_IM = tbName("im");

    public static final String ES_INDEX_BIGDATA_USER_IMAGE = tbName("bigdata-user-image");
    public static final String ES_TYPE_USER_IMAGE = tbName("user-image");
}
