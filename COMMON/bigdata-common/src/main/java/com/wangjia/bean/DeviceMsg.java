package com.wangjia.bean;

import com.google.gson.JsonObject;

/**
 * Created by Administrator on 2017/4/28.
 */
public class DeviceMsg extends ObjectBean {
    protected int dType = 0;
    protected String uuid = "";
    protected String deviceId = "";
    protected String name = "";
    protected String system = "";
    protected int sw = 0;
    protected int sh = 0;
    protected int acc = 1;

    public int getdType() {
        return dType;
    }

    public void setdType(int dType) {
        this.dType = dType;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
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

    @Override
    public JsonObject toJson() {
        JsonObject obj = new JsonObject();
        obj.addProperty("type", dType);
        obj.addProperty("uuid", uuid);
        obj.addProperty("deviceId", deviceId);
        obj.addProperty("name", name);
        obj.addProperty("system", system);
        obj.addProperty("sw", sw);
        obj.addProperty("sh", sh);
        obj.addProperty("acc", acc);

        return obj;
    }
}
