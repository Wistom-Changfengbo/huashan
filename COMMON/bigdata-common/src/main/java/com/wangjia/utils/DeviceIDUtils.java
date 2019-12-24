package com.wangjia.utils;

import com.wangjia.common.DeviceType;

/**
 * Created by Administrator on 2017/10/24.
 */
public final class DeviceIDUtils {

    /**
     * 是否是错误的IMEI
     *
     * @param imei
     * @return
     */
    public static boolean isErrorIMEI(String imei) {
        if (imei.length() != 15)
            return true;
        int num = 0;
        byte[] bs = imei.getBytes();
        for (byte b : bs) {
            if (b < '0' || b > '9')
                return true;
            if (b == '0')
                num += 1;
        }
        if (num == 15)
            return true;
        return false;
    }

    public static boolean isErrorSystemId(String systemId) {
        if (systemId.length() < 15)
            return true;
        return false;
    }

    /**
     * 是否是错误的IDFA
     *
     * @param idfa
     * @return
     */
    public static boolean isErrorIOSIDFA(String idfa) {
        if (idfa.length() != 36)
            return true;
        if (idfa.equals("00000000-0000-0000-0000-000000000000"))
            return true;
        return false;
    }

    /**
     * 得到Android imei
     *
     * @param id
     * @return
     */
    public static String getAndroidImei(String id) {
        /*
        if (id.length() != 31)
            return null;
        String imei = id.substring(0, 15);
        if (isErrorIMEI(imei))
            return null;
        return imei;
        */
        String[] split = id.split("#");
        if (split.length == 2)
            return split[0];
        return null;
    }

    /**
     * 得到Android 系统ID
     *
     * @param id
     * @return
     */
    public static String getAndroidSystemId(String id) {
        /*
        if (id.length() != 31)
            return null;
        String androidId = id.substring(15);
        return androidId;
        */
        String[] split = id.split("#");
        if (split.length == 2)
            return split[1];
        return null;
    }

    /**
     * 是否是错误的浏览器ID
     *
     * @param id
     * @return
     */
    public static boolean isErrorBrowserID(String id) {
        byte[] bs = id.getBytes();
        if (bs.length != 32)
            return true;
        for (byte b : bs) {
            if (b == '=' || b == '_')
                return true;
        }
        return false;
    }

    /**
     * 是否是错误的AndroidID
     *
     * @param id
     * @return
     */
    public static boolean isErrorAndroidID(String id) {
        if (id.indexOf('#') > -1)
            return false;
        return true;
    }

    /**
     * 是否是错误的IOSID
     *
     * @param id
     * @return
     */
    public static boolean isErrorIOSID(String id) {
        return isErrorIOSIDFA(id);
    }

    /**
     * 是否是错误ID
     *
     * @param deviceid
     * @param id
     * @return
     */
    public static boolean isErrorID(int deviceid, String id) {
        switch (deviceid) {
            case DeviceType.DEVICE_PC_BROWSER:
                return isErrorBrowserID(id);
            case DeviceType.DEVICE_PHONE_ANDROID:
                return isErrorAndroidID(id);
            case DeviceType.DEVICE_PHONE_IOS:
                return isErrorIOSID(id);
        }
        return false;
    }

    public static boolean isErrorID(String deviceName, String id) {
        switch (deviceName) {
            case "web":
            case "WEB":
                return isErrorBrowserID(id);
            case "android":
            case "ANDROID":
                return isErrorAndroidID(id);
            case "ios":
            case "IOS":
                return isErrorIOSID(id);
        }
        return false;
    }

    /**
     * 修正AndroidID
     *
     * @param id
     * @return
     */
    public static String normalAndroidID(String id) {
        if (!isErrorAndroidID(id))
            return id;
        char[] cs = id.toCharArray();
        String imei = "", systemId = "";
        if (cs.length == 31) {
            imei = new String(cs, 0, 15);
            systemId = new String(cs, 15, 16);
        } else if (cs.length == 30) {
            imei = new String(cs, 0, 14);
            systemId = new String(cs, 14, 16);
        } else if (cs.length == 29) {
            imei = new String(cs, 0, 14);
            systemId = new String(cs, 14, 15);
        } else {
            return "";
        }

        if (imei.length() != 15) {
            imei = ImeiUtils.getImeiBy14(imei);
        }
        if (imei.length() != 15 || systemId.length() < 15)
            return "";
        return imei + '#' + systemId;
    }

    public static String normalAndroidID(String imei, String systemId) {
        if (imei.length() != 15) {
            imei = ImeiUtils.getImeiBy14(imei);
        }
        if (imei.length() != 15 || systemId.length() < 15)
            return "";
        return imei + '#' + systemId;
    }

    public static String normalDeviceID(int deviceid, String id) {
        switch (deviceid) {
            case DeviceType.DEVICE_PC_BROWSER:
                return id;
            case DeviceType.DEVICE_PHONE_ANDROID:
                return normalAndroidID(id);
            case DeviceType.DEVICE_PHONE_IOS:
                return id;
        }
        return "";
    }

    public static boolean isErrorUUID(String uuid) {
        if (uuid != null && uuid.length() == 32)
            return false;
        return true;
        /*
        byte[] bs = uuid.getBytes();
        if (bs.length != 16)
            return true;
        for (byte b : bs) {
            if (b == '#')
                return true;
        }
        return false;
        */
    }

    public static String cookie2UUID(String cookieId) {
        if (isErrorBrowserID(cookieId))
            return "";
        return MD5Utils.MD5(cookieId);
    }

    public static String androidID2UUID(String androidId) {
        if (isErrorAndroidID(androidId))
            return "";
        return MD5Utils.MD5(androidId);
    }

    public static String iosIDFA2UUID(String idfa) {
        if (isErrorIOSID(idfa))
            return "";
        return MD5Utils.MD5(idfa);
    }

}
