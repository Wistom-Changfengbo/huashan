package com.wangjia.bean.search;

import java.io.Serializable;

public class UserRequestParam implements Serializable {
    private int     page;
    private int     pageSize;
    private int     sex = -10;

    private String key;
    private int sort;

    private int  minConsume;
    private int  maxConsume;

    private String  userTag;

    private String  appTag;

    private String  platform;

    private long  minFirsttime;
    private long  maxFirsttime;

    private float  minLiveness;
    private float  maxLiveness;

    private int     bclue = -10;

    private int     minPageNum;
    private int     maxPageNum;

    private int     appId;
    private float  appTagValue;
    private float  userLabelValue;

    private int  minActiveDayNum;
    private int  maxActiveDayNum;

    private String address;
    private String appPkgList;

    private long minStayTimeSum;
    private long maxStayTimeSum;

    private long minActionTime;
    private long maxActionTime;

    private String deviceid;

    private long minr7ActiveDayNum;
    private long maxr7ActiveDayNum;

    private String ip;
    private String userLabelKey;

    private long    minLastTime;
    private long    maxLastTime;

    private String  appNameList;
    private String  userTagKey;

    private int     eventIdValue;
    private String  deviceName;

    private String  eventIdKey;

    public int getPage() {
        return page;
    }

    public void setPage(int page) {
        this.page = page;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public int getSex() {
        return sex;
    }

    public void setSex(int sex) {
        this.sex = sex;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public int getSort() {
        return sort;
    }

    public void setSort(int sort) {
        this.sort = sort;
    }

    public int getMinConsume() {
        return minConsume;
    }

    public void setMinConsume(int minConsume) {
        this.minConsume = minConsume;
    }

    public int getMaxConsume() {
        return maxConsume;
    }

    public void setMaxConsume(int maxConsume) {
        this.maxConsume = maxConsume;
    }

    public String getUserTag() {
        return userTag;
    }

    public void setUserTag(String userTag) {
        this.userTag = userTag;
    }

    public String getAppTag() {
        return appTag;
    }

    public void setAppTag(String appTag) {
        this.appTag = appTag;
    }

    public String getPlatform() {
        return platform;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }

    public long getMinFirsttime() {
        return minFirsttime;
    }

    public void setMinFirsttime(long minFirsttime) {
        this.minFirsttime = minFirsttime;
    }

    public long getMaxFirsttime() {
        return maxFirsttime;
    }

    public void setMaxFirsttime(long maxFirsttime) {
        this.maxFirsttime = maxFirsttime;
    }

    public float getMinLiveness() {
        return minLiveness;
    }

    public void setMinLiveness(float minLiveness) {
        this.minLiveness = minLiveness;
    }

    public float getMaxLiveness() {
        return maxLiveness;
    }

    public void setMaxLiveness(float maxLiveness) {
        this.maxLiveness = maxLiveness;
    }

    public int getBclue() {
        return bclue;
    }

    public void setBclue(int bclue) {
        this.bclue = bclue;
    }

    public int getMinPageNum() {
        return minPageNum;
    }

    public void setMinPageNum(int minPageNum) {
        this.minPageNum = minPageNum;
    }

    public int getMaxPageNum() {
        return maxPageNum;
    }

    public void setMaxPageNum(int maxPageNum) {
        this.maxPageNum = maxPageNum;
    }

    public int getAppId() {
        return appId;
    }

    public void setAppId(int appId) {
        this.appId = appId;
    }

    public float getAppTagValue() {
        return appTagValue;
    }

    public void setAppTagValue(float appTagValue) {
        this.appTagValue = appTagValue;
    }

    public float getUserLabelValue() {
        return userLabelValue;
    }

    public void setUserLabelValue(float userLabelValue) {
        this.userLabelValue = userLabelValue;
    }

    public int getMinActiveDayNum() {
        return minActiveDayNum;
    }

    public void setMinActiveDayNum(int minActiveDayNum) {
        this.minActiveDayNum = minActiveDayNum;
    }

    public int getMaxActiveDayNum() {
        return maxActiveDayNum;
    }

    public void setMaxActiveDayNum(int maxActiveDayNum) {
        this.maxActiveDayNum = maxActiveDayNum;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getAppPkgList() {
        return appPkgList;
    }

    public void setAppPkgList(String appPkgList) {
        this.appPkgList = appPkgList;
    }

    public long getMinStayTimeSum() {
        return minStayTimeSum;
    }

    public void setMinStayTimeSum(long minStayTimeSum) {
        this.minStayTimeSum = minStayTimeSum;
    }

    public long getMaxStayTimeSum() {
        return maxStayTimeSum;
    }

    public void setMaxStayTimeSum(long maxStayTimeSum) {
        this.maxStayTimeSum = maxStayTimeSum;
    }

    public long getMinActionTime() {
        return minActionTime;
    }

    public void setMinActionTime(long minActionTime) {
        this.minActionTime = minActionTime;
    }

    public long getMaxActionTime() {
        return maxActionTime;
    }

    public void setMaxActionTime(long maxActionTime) {
        this.maxActionTime = maxActionTime;
    }

    public String getDeviceid() {
        return deviceid;
    }

    public void setDeviceid(String deviceid) {
        this.deviceid = deviceid;
    }

    public long getMinr7ActiveDayNum() {
        return minr7ActiveDayNum;
    }

    public void setMinr7ActiveDayNum(long minr7ActiveDayNum) {
        this.minr7ActiveDayNum = minr7ActiveDayNum;
    }

    public long getMaxr7ActiveDayNum() {
        return maxr7ActiveDayNum;
    }

    public void setMaxr7ActiveDayNum(long maxr7ActiveDayNum) {
        this.maxr7ActiveDayNum = maxr7ActiveDayNum;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getUserLabelKey() {
        return userLabelKey;
    }

    public void setUserLabelKey(String userLabelKey) {
        this.userLabelKey = userLabelKey;
    }

    public long getMinLastTime() {
        return minLastTime;
    }

    public void setMinLastTime(long minLastTime) {
        this.minLastTime = minLastTime;
    }

    public long getMaxLastTime() {
        return maxLastTime;
    }

    public void setMaxLastTime(long maxLastTime) {
        this.maxLastTime = maxLastTime;
    }

    public String getAppNameList() {
        return appNameList;
    }

    public void setAppNameList(String appNameList) {
        this.appNameList = appNameList;
    }

    public String getUserTagKey() {
        return userTagKey;
    }

    public void setUserTagKey(String userTagKey) {
        this.userTagKey = userTagKey;
    }

    public int getEventIdValue() {
        return eventIdValue;
    }

    public void setEventIdValue(int eventIdValue) {
        this.eventIdValue = eventIdValue;
    }

    public String getDeviceName() {
        return deviceName;
    }

    public void setDeviceName(String deviceName) {
        this.deviceName = deviceName;
    }

    public String getEventIdKey() {
        return eventIdKey;
    }

    public void setEventIdKey(String eventIdKey) {
        this.eventIdKey = eventIdKey;
    }

    @Override
    public String toString() {
        return "UserRequestParam{" +
                "page=" + page +
                ", pageSize=" + pageSize +
                ", sex=" + sex +
                ", key='" + key + '\'' +
                ", sort=" + sort +
                ", minConsume=" + minConsume +
                ", maxConsume=" + maxConsume +
                ", userTag='" + userTag + '\'' +
                ", appTag='" + appTag + '\'' +
                ", platform='" + platform + '\'' +
                ", minFirsttime=" + minFirsttime +
                ", maxFirsttime=" + maxFirsttime +
                ", minLiveness=" + minLiveness +
                ", maxLiveness=" + maxLiveness +
                ", bclue=" + bclue +
                ", minPageNum=" + minPageNum +
                ", maxPageNum=" + maxPageNum +
                ", appId=" + appId +
                ", appTagValue=" + appTagValue +
                ", userLabelValue=" + userLabelValue +
                ", minActiveDayNum=" + minActiveDayNum +
                ", maxActiveDayNum=" + maxActiveDayNum +
                ", address='" + address + '\'' +
                ", appPkgList='" + appPkgList + '\'' +
                ", minStayTimeSum=" + minStayTimeSum +
                ", maxStayTimeSum=" + maxStayTimeSum +
                ", minActionTime=" + minActionTime +
                ", maxActionTime=" + maxActionTime +
                ", deviceid='" + deviceid + '\'' +
                ", minr7ActiveDayNum=" + minr7ActiveDayNum +
                ", maxr7ActiveDayNum=" + maxr7ActiveDayNum +
                ", ip='" + ip + '\'' +
                ", userLabelKey='" + userLabelKey + '\'' +
                ", minLastTime=" + minLastTime +
                ", maxLastTime=" + maxLastTime +
                ", appNameList='" + appNameList + '\'' +
                ", userTagKey='" + userTagKey + '\'' +
                ", eventIdValue=" + eventIdValue +
                ", deviceName='" + deviceName + '\'' +
                ", eventIdKey='" + eventIdKey + '\'' +
                '}';
    }
}
