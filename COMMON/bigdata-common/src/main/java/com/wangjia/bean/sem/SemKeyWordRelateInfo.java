package com.wangjia.bean.sem;

import java.io.Serializable;

/**
 * Created by Cfb on 2018/5/18.
 */
public class SemKeyWordRelateInfo implements Serializable {
    private String appid;
    private String sourceid;
    private String query;
    private Long keywordId;
    private Long creativeId;
    private int display;
    private int click;
    private float cost;
    private float clickRate;
    private String timeStamp;

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getAppid() {
        return appid;
    }

    public void setAppid(String appid) {
        this.appid = appid;
    }

    public String getSourceid() {
        return sourceid;
    }

    public void setSourceid(String sourceid) {
        this.sourceid = sourceid;
    }

    public Long getKeywordId() {
        return keywordId;
    }

    public void setKeywordId(Long keywordId) {
        this.keywordId = keywordId;
    }

    public Long getCreativeId() {
        return creativeId;
    }

    public void setCreativeId(Long creativeId) {
        this.creativeId = creativeId;
    }

    public int getDisplay() {
        return display;
    }

    public void setDisplay(int display) {
        this.display = display;
    }

    public int getClick() {
        return click;
    }

    public void setClick(int click) {
        this.click = click;
    }

    public float getCost() {
        return cost;
    }

    public void setCost(float cost) {
        this.cost = cost;
    }

    public float getClickRate() {
        return clickRate;
    }

    public void setClickRate(float clickRate) {
        this.clickRate = clickRate;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
    }

    @Override
    public String toString() {
        return "SemKeyWordRelateInfo{" +
                "appid='" + appid + '\'' +
                ", sourceid='" + sourceid + '\'' +
                ", keywordId=" + keywordId +
                ", creativeId=" + creativeId +
                ", display=" + display +
                ", click=" + click +
                ", cost=" + cost +
                ", clickRate=" + clickRate +
                ", timeStamp='" + timeStamp + '\'' +
                '}';
    }
}
