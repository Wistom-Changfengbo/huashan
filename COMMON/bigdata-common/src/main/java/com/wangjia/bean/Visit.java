package com.wangjia.bean;

import com.google.gson.JsonObject;

import java.io.Serializable;

/**
 * Created by Administrator on 2017/5/5.
 */
public class Visit extends ObjectBean implements Comparable<Visit>, Serializable {
    protected String mainKey = "";
    protected String uuid = "";
    protected String ip = "";
    protected String ref = "";
    protected String address = "";
    protected long start = 0;
    protected long time = 0;
    protected int num = 0;
    protected int platform = 1;
    protected String appid = "";
    protected String version = "-";
    protected String net = "-";
    protected String deviceId = "";
    protected String eventIds = "";
    protected int eNum = 0;
    protected String appName;

    public String getMainKey() {
        return mainKey;
    }

    public void setMainKey(String mainKey) {
        this.mainKey = mainKey;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getRef() {
        return ref;
    }

    public void setRef(String ref) {
        this.ref = ref;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    public int getPlatform() {
        return platform;
    }

    public void setPlatform(int platform) {
        this.platform = platform;
    }

    public String getAppid() {
        return appid;
    }

    public void setAppid(String appid) {
        this.appid = appid;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getNet() {
        return net;
    }

    public void setNet(String net) {
        this.net = net;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public int geteNum() {
        return eNum;
    }

    public void seteNum(int eNum) {
        this.eNum = eNum;
    }

    public String getEventIds() {
        return eventIds;
    }

    public void setEventIds(String eventIds) {
        this.eventIds = eventIds;
    }


    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    @Override
    public JsonObject toJson() {
        JsonObject obj = new JsonObject();
        obj.addProperty("deviceId", deviceId);
        obj.addProperty("uuid", uuid);
        obj.addProperty("ip", ip);
        obj.addProperty("ref", ref);
        obj.addProperty("address", address);
        obj.addProperty("start", start);
        obj.addProperty("time", time);
        obj.addProperty("num", num);
        obj.addProperty("platform", platform);
        obj.addProperty("appid", appid);
        obj.addProperty("eNum", eNum);
        obj.addProperty("version", version);
        obj.addProperty("net", net);
        obj.addProperty("eventIds", eventIds);

        return obj;
    }

    @Override
    public String toString() {
        return toJson().toString();
    }

    @Override
    public int compareTo(Visit o) {
        if (start == (long)o.getStart()) {
            return 0;
        }

        return (start - (long)o.getStart()) > 0 ? -1 : 1;
    }
}
