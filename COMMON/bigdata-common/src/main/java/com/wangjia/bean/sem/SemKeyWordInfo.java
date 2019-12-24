package com.wangjia.bean.sem;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by Cfb on 2018/5/16.
 */
public class SemKeyWordInfo implements Serializable{
    private String appid;
    private String sourceid;
    private Long keywordId;
    private Long creativeId;
    private String username;
    private String campaignName;
    private String adgroup;
    private String keyword;
    private String creativeTitle;
    private String creativeDesc1;
    private String creativeDesc2;
    private String creativeUrl;
    private String timeStamp;
    private int display;
    private int click;
    private float cost;
    private float costAvg;
    private float clickRate;
    private float cpm;
    private int conversion;
    private Date day;
    private Date addtime;

    public String getSourceid() {
        return sourceid;
    }

    public void setSourceid(String sourceid) {
        this.sourceid = sourceid;
    }

    public String getAppid() {
        return appid;
    }

    public void setAppid(String appid) {
        this.appid = appid;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
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

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getCampaignName() {
        return campaignName;
    }

    public void setCampaignName(String campaignName) {
        this.campaignName = campaignName;
    }

    public String getAdgroup() {
        return adgroup;
    }

    public void setAdgroup(String adgroup) {
        this.adgroup = adgroup;
    }

    public String getKeyword() {
        return keyword;
    }

    public void setKeyword(String keyword) {
        this.keyword = keyword;
    }

    public String getCreativeTitle() {
        return creativeTitle;
    }

    public void setCreativeTitle(String creativeTitle) {
        this.creativeTitle = creativeTitle;
    }

    public String getCreativeDesc1() {
        return creativeDesc1;
    }

    public void setCreativeDesc1(String creativeDesc1) {
        this.creativeDesc1 = creativeDesc1;
    }

    public String getCreativeDesc2() {
        return creativeDesc2;
    }

    public void setCreativeDesc2(String creativeDesc2) {
        this.creativeDesc2 = creativeDesc2;
    }

    public String getCreativeUrl() {
        return creativeUrl;
    }

    public void setCreativeUrl(String creativeUrl) {
        this.creativeUrl = creativeUrl;
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

    public float getCostAvg() {
        return costAvg;
    }

    public void setCostAvg(float costAvg) {
        this.costAvg = costAvg;
    }

    public float getClickRate() {
        return clickRate;
    }

    public void setClickRate(float clickRate) {
        this.clickRate = clickRate;
    }

    public float getCpm() {
        return cpm;
    }

    public void setCpm(float cpm) {
        this.cpm = cpm;
    }

    public int getConversion() {
        return conversion;
    }

    public void setConversion(int conversion) {
        this.conversion = conversion;
    }

    public Date getDay() {
        return day;
    }

    public void setDay(Date day) {
        this.day = day;
    }

    public Date getAddtime() {
        return addtime;
    }

    public void setAddtime(Date addtime) {
        this.addtime = addtime;
    }

    @Override
    public String toString() {
        return "SemKeyWordInfo{" +
                "appid='" + appid + '\'' +
                ", sourceid='" + sourceid + '\'' +
                ", keywordId=" + keywordId +
                ", creativeId=" + creativeId +
                ", username='" + username + '\'' +
                ", campaignName='" + campaignName + '\'' +
                ", adgroup='" + adgroup + '\'' +
                ", keyword='" + keyword + '\'' +
                ", creativeTitle='" + creativeTitle + '\'' +
                ", creativeDesc1='" + creativeDesc1 + '\'' +
                ", creativeDesc2='" + creativeDesc2 + '\'' +
                ", creativeUrl='" + creativeUrl + '\'' +
                ", timeStamp='" + timeStamp + '\'' +
                ", display=" + display +
                ", click=" + click +
                ", cost=" + cost +
                ", costAvg=" + costAvg +
                ", clickRate=" + clickRate +
                ", cpm=" + cpm +
                ", conversion=" + conversion +
                ", day=" + day +
                ", addtime=" + addtime +
                '}';
    }
}
