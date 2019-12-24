package com.wangjia.hbase;

import com.wangjia.utils.MD5Utils;
import org.apache.hadoop.hbase.util.Bytes;

import java.nio.charset.Charset;

/**
 * Created by Administrator on 2017/4/24.
 */
public final class ExHbase {

    private static final String[] placeholder = new String[20];
    private static final long MAX_TIME_MILLIS = 9000000000000L;

    static {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < placeholder.length; i++) {
            sb.append('0');
            placeholder[i] = sb.toString();
        }
    }

    /**
     * 得到访问轨迹的RowKey
     *
     * @param appid
     * @param uuid
     * @return
     */
    public static String getVisitKey(String appid, String uuid, long time) {
        time = MAX_TIME_MILLIS - time;
        return String.format("%s%013d%s", uuid, time, appid);
    }

    /**
     * 得到访问轨迹详情的RowKey
     *
     * @param appid
     * @param uuid
     * @param time  开始时间戳
     * @return
     */
    public static String getVisitDesKey(String appid, String uuid, long time) {
        time = MAX_TIME_MILLIS - time;
        return String.format("%s%013d%s", uuid, time, appid);
    }

    /**
     * 得到userid to uuid表的RowKey
     *
     * @param userId
     * @param accountId 帐号系统ID
     * @return
     */
    public static String getUserKey(String userId, int accountId) {
        int size = 20;
        size -= userId.length();
        //size -= 1;
        //size -= 3;
        size -= 4;

        StringBuilder sb = new StringBuilder();
        sb.append(userId);
        sb.reverse();
        sb.append(']');
        if (size > 1) {
            sb.append(placeholder[size - 1]);
        }
        sb.append(String.format("%03d", accountId));

        return sb.toString();
    }

    /**
     * 得到GPS的RowKey
     *
     * @param uuid
     * @param time
     * @return
     */
    public static String getGPSKey(String uuid, long time) {
        time = MAX_TIME_MILLIS - time;
        return String.format("%s%013d", uuid, time);
    }

    /**
     * 得到消息详情的RowKey
     *
     * @param appid    appid
     * @param etype    事件大类型
     * @param sub_type 事件小类型
     * @param time     时间戳
     * @return
     */
    public static String getEventDesKey(String appid, String etype, String sub_type, long time) {
        StringBuffer sb = new StringBuffer();
        sb.append(appid);
        sb.append('#');
        sb.append(etype);
        sb.append('#');
        sb.append(sub_type);
        time = MAX_TIME_MILLIS - time;
        return String.format("%s%013d", MD5Utils.MD5(sb.toString()), time);
    }

    /**
     * 得到手机的RowKey
     *
     * @param phone
     * @return
     */
    public static String getPhoneKey(String phone) {
        StringBuilder sb = new StringBuilder();
        sb.append(phone);
        sb.reverse();
        return sb.toString();
    }

    /**
     * 得到IMKey
     *
     * @param deviceid  设备ID
     * @param client_id 租户ID
     * @param time      时间戳
     * @return
     */
    public static String getIMKey(String deviceid, String client_id, long time) {
        time = MAX_TIME_MILLIS - time;
        StringBuilder sb = new StringBuilder();
        sb.append(deviceid);
        sb.append("::");
        sb.append(client_id);
        String uuid = MD5Utils.MD5(sb.toString());
        return String.format("%s%013d", uuid, time);
    }

    public static void main(String[] args) {
        System.out.println(getEventDesKey("1200","40","__sys_app_applist",1552621239866l));
        System.out.println();
    }


}
