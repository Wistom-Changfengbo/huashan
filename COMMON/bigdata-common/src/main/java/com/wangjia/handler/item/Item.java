package com.wangjia.handler.item;

import java.io.Serializable;

/**
 * Created by Administrator on 2018/1/19.
 */
public class Item implements Serializable {
    private static final long serialVersionUID = -281077803391641010L;

    protected String appid;
    protected int companyid;
    protected int ruleid;
    protected int itemType;
    protected String itemid;
    protected float baseValue;
    protected long stayTime;

    /**
     * @param appid     APPID
     * @param companyid 公司ID
     * @param ruleid    规则ID
     * @param itemType  物品类型
     * @param itemid    物品ID
     * @param baseValue 基础得分
     * @param stayTime  停留时间
     */
    public Item(String appid, int companyid, int ruleid, int itemType, String itemid, float baseValue, long stayTime) {
        this.appid = appid;
        this.companyid = companyid;
        this.ruleid = ruleid;
        this.itemType = itemType;
        this.itemid = itemid;
        this.baseValue = baseValue;
        this.stayTime = stayTime;
    }

    public Item() {
    }

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public String getAppid() {
        return appid;
    }

    public void setAppid(String appid) {
        this.appid = appid;
    }

    public int getCompanyid() {
        return companyid;
    }

    public void setCompanyid(int companyid) {
        this.companyid = companyid;
    }

    public int getRuleid() {
        return ruleid;
    }

    public void setRuleid(int ruleid) {
        this.ruleid = ruleid;
    }

    public int getItemType() {
        return itemType;
    }

    public void setItemType(int itemType) {
        this.itemType = itemType;
    }

    public String getItemid() {
        return itemid;
    }

    public void setItemid(String itemid) {
        this.itemid = itemid;
    }

    public float getBaseValue() {
        return baseValue;
    }

    public void setBaseValue(float baseValue) {
        this.baseValue = baseValue;
    }

    public long getStayTime() {
        return stayTime;
    }

    public void setStayTime(long stayTime) {
        this.stayTime = stayTime;
    }

    @Override
    public String toString() {
        return "Item{" +
                "appid='" + appid + '\'' +
                ", companyid=" + companyid +
                ", ruleid=" + ruleid +
                ", itemType=" + itemType +
                ", itemid='" + itemid + '\'' +
                ", baseValue=" + baseValue +
                ", stayTime=" + stayTime +
                '}';
    }
}
