package com.wangjia.bean.search;

import java.io.Serializable;

public class UserProfileRequest implements Serializable {
    private int     page;
    private int     pageSize;
    private int  sex = -10;
    private int  minConsume;
    private int  maxConsume;
    private long    beginUpdateTime;
    private long    endUpdateTime;
    private String  userTag;
    private String  appTag;
    private String  platform;


    //排序字段
    private String key;
    //排序
    private int sort;

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

    public long getBeginUpdateTime() {
        return beginUpdateTime;
    }

    public void setBeginUpdateTime(long beginUpdateTime) {
        this.beginUpdateTime = beginUpdateTime;
    }

    public long getEndUpdateTime() {
        return endUpdateTime;
    }

    public void setEndUpdateTime(long endUpdateTime) {
        this.endUpdateTime = endUpdateTime;
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


    @Override
    public String toString() {
        return "UserProfileRequest{" +
                "page=" + page +
                ", pageSize=" + pageSize +
                ", sex=" + sex +
                ", minConsume=" + minConsume +
                ", maxConsume=" + maxConsume +
                ", beginUpdateTime=" + beginUpdateTime +
                ", endUpdateTime=" + endUpdateTime +
                ", userTag='" + userTag + '\'' +
                ", appTag='" + appTag + '\'' +
                ", platform='" + platform + '\'' +
                ", key='" + key + '\'' +
                ", sort='" + sort + '\'' +
                '}';
    }
}
