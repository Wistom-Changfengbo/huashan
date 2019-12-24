package com.wangjia.bean;

import com.google.gson.JsonObject;

/**
 * Created by Administrator on 2017/4/28.
 */
public class WebDeviceMsg extends DeviceMsg {
    protected int bCookie;
    protected int bFlash;

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
        obj.addProperty("bCookie", bCookie);
        obj.addProperty("bFlash", bFlash);

        return obj;
    }

    @Override
    public String toString() {
        return "WebDeviceMsg{" +
                "bCookie=" + bCookie +
                ", bFlash=" + bFlash +
                ", dType=" + dType +
                ", uuid='" + uuid + '\'' +
                ", deviceId='" + deviceId + '\'' +
                ", name='" + name + '\'' +
                ", system='" + system + '\'' +
                ", sw=" + sw +
                ", sh=" + sh +
                ", acc=" + acc +
                '}';
    }
}
