package com.wangjia.bean;

import java.io.Serializable;

public class EventDesc implements Serializable {
    private String deviceid;
    private String ip;
    private String msg;
    private String ref;
    private Long time;
    private String url;
    private String userid;
    private String uuid;

    public EventDesc(String deviceid, String ip, String msg, String ref, Long time, String url, String userid, String uuid) {
        this.deviceid = deviceid;
        this.ip = ip;
        this.msg = msg;
        this.ref = ref;
        this.time = time;
        this.url = url;
        this.userid = userid;
        this.uuid = uuid;
    }

    public String getDeviceid() {
        return deviceid;
    }

    public void setDeviceid(String deviceid) {
        this.deviceid = deviceid;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public String getRef() {
        return ref;
    }

    public void setRef(String ref) {
        this.ref = ref;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUserid() {
        return userid;
    }

    public void setUserid(String userid) {
        this.userid = userid;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    @Override
    public String toString() {
        return "EventDesc{" +
                "deviceid='" + deviceid + '\'' +
                ", ip='" + ip + '\'' +
                ", msg='" + msg + '\'' +
                ", ref='" + ref + '\'' +
                ", time=" + time +
                ", url='" + url + '\'' +
                ", userid='" + userid + '\'' +
                ", uuid='" + uuid + '\'' +
                '}';
    }
}
