package com.wangjia.bean;

import java.io.Serializable;
import java.util.List;

/**
 * Created by Cfb on 2017/7/5.
 */
public class EsAppListDocBean implements Serializable {

    private List<String> pkgList;
    private List<String> appNameList;
    private long time;
    private String platform;
    private String uuid;
    private String deviceid;

    public EsAppListDocBean(List<String> pkgList, List<String> appNameList, long time, String platform, String uuid, String deviceid) {
        this.pkgList = pkgList;
        this.appNameList = appNameList;
        this.time = time;
        this.platform = platform;
        this.uuid = uuid;
        this.deviceid = deviceid;
    }

    public List<String> getPkgList() {
        return pkgList;
    }

    public void setPkgList(List<String> pkgList) {
        this.pkgList = pkgList;
    }

    public List<String> getAppNameList() {
        return appNameList;
    }

    public void setAppNameList(List<String> appNameList) {
        this.appNameList = appNameList;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public String getPlatform() {
        return platform;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getDeviceid() {
        return deviceid;
    }

    public void setDeviceid(String deviceid) {
        this.deviceid = deviceid;
    }
}
