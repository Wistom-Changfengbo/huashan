package com.wangjia.bean;

import com.wangjia.handler.idcenter.IDName;
import com.wangjia.handler.idcenter.IDType;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2017/11/8.
 * <p>
 * 设备描述类
 */
public class DeviceDes implements Serializable {
    /**
     * 第一次时间
     */
    protected long firstTime = Long.MAX_VALUE;

    /**
     * 最后一次时间
     */
    protected long lastTime = 0;

    /**
     * 设备ID
     */
    protected String deviceId = "";

    /**
     * UUID
     */
    protected String uuid = "";

    /**
     * 设备类型
     */
    protected int type = 0;

    /**
     * 设备名
     */
    protected String name = "";

    /**
     * 系统名
     */
    protected String system = "";

    /**
     * 宽
     */
    protected int sw = 0;

    /**
     * 高
     */
    protected int sh = 0;

    /**
     * 是否有认证
     */
    protected int acc = 0;

    /**
     * 是否支持cookie
     */
    protected int bCookie = 0;

    /**
     * 是否支持flash
     */
    protected int bFlash = 0;

    /**
     * 设备ID
     * <p>
     * mac#xxxxxxxxxxx
     * cookie#xxxxxxxxxx
     * idfa#xxxxxxxxxxxxxxx
     * imei#xxxxxxxxxxxxxxx
     * systemId#xxxxxxxxxxxxx
     */
    protected Map<String, Long> ids = new HashMap<>();

    public void setTime(long time) {
        if (firstTime > time)
            firstTime = time;
        if (lastTime < time)
            lastTime = time;
    }

    public void addKey(String key, long time) {
        setTime(time);
        long oldTime = ids.getOrDefault(key, Long.MAX_VALUE);
        if (oldTime > time) {
            ids.put(key, time);
        }
    }

    public void addUserId(String userId, long time) {
        String key = "_userid#" + userId;
        addKey(key, time);
    }

    public void addAppId(String appid, long time) {
        String key = "_appid#" + appid;
        addKey(key, time);
    }

    public void addPhone(String number, long time) {
        String key = "_phone#" + number;
        addKey(key, time);
    }

    public void addId(String idName, String id, long time) {
        String key = "_did#" + idName + '#' + id;
        addKey(key, time);
    }

    public void addId(int idType, String id, long time) {
        switch (idType) {
            case IDType.ID_MAC:
                addId(IDName.ID_MAC, id, time);
                break;
            case IDType.ID_COOKIE:
                addId(IDName.ID_COOKIE, id, time);
                break;
            case IDType.ID_IOS_IDFA:
                addId(IDName.ID_IOS_IDFA, id, time);
                break;
            case IDType.ID_ANDROID_IMEI:
                addId(IDName.ID_ANDROID_IMEI, id, time);
                break;
            case IDType.ID_ANDROID_SYSTEMID:
                addId(IDName.ID_ANDROID_SYSTEMID, id, time);
                break;
        }
    }


    public long getFirstTime() {
        return firstTime;
    }

    public void setFirstTime(long firstTime) {
        if (firstTime < this.firstTime)
            this.firstTime = firstTime;
    }

    public long getLastTime() {
        return lastTime;
    }

    public void setLastTime(long lastTime) {
        if (lastTime > this.lastTime)
            this.lastTime = lastTime;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSystem() {
        return system;
    }

    public void setSystem(String system) {
        this.system = system;
    }

    public int getSw() {
        return sw;
    }

    public void setSw(int sw) {
        this.sw = sw;
    }

    public int getSh() {
        return sh;
    }

    public void setSh(int sh) {
        this.sh = sh;
    }

    public int getAcc() {
        return acc;
    }

    public void setAcc(int acc) {
        this.acc = acc;
    }

    public int getbCookie() {
        return bCookie;
    }

    public void setbCookie(int bCookie) {
        this.bCookie = bCookie;
    }

    public int getbFlash() {
        return bFlash;
    }

    public void setbFlash(int bFlash) {
        this.bFlash = bFlash;
    }

    public Map<String, Long> getIds() {
        return ids;
    }

    public void setIds(Map<String, Long> ids) {
        this.ids = ids;
    }


    @Override
    public String toString() {
        return "DeviceDes{" +
                "firstTime=" + firstTime +
                ", lastTime=" + lastTime +
                ", deviceId='" + deviceId + '\'' +
                ", uuid='" + uuid + '\'' +
                ", type=" + type +
                ", name='" + name + '\'' +
                ", system='" + system + '\'' +
                ", sw=" + sw +
                ", sh=" + sh +
                ", acc=" + acc +
                ", bCookie=" + bCookie +
                ", bFlash=" + bFlash +
                ", ids=" + ids +
                '}';
    }
}
