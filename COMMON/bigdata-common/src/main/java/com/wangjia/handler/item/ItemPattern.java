package com.wangjia.handler.item;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.io.Serializable;
import java.util.regex.Matcher;

/**
 * Created by Administrator on 2018/1/19.
 */
public class ItemPattern implements Serializable {
    private static final long serialVersionUID = -6191876467510868816L;

    /**
     * 页面行为
     */
    public static final int BEHAVIOR_TYPE_PAGE = 50;
    /**
     * 事件行为
     */
    public static final int BEHAVIOR_TYPE_EVENT = 40;


    protected int id;
    protected String appid;
    protected int companyid;
    protected int itemTypeId;
    protected float starLevel;
    protected int behaviorType;
    protected int behaviorRuleType;
    protected String behaviorRule;
    protected int dataRuleType;
    protected String dataRule;
    protected int weight;
    protected String des;
    protected int active;
    protected long addtime;

    /**
     * 页面匹配正则
     */
    protected java.util.regex.Pattern regexPage = null;

    /**
     * 数据匹配正则
     */
    protected java.util.regex.Pattern regexData = null;

    /**
     * JSON数据路径
     */
    protected String[] jsonDataPath = null;


    /**
     * @param id               ID
     * @param appid            APPID
     * @param companyid        公司ID
     * @param itemTypeId       物品类型
     * @param starLevel        星级
     * @param behaviorType     行为类型
     * @param behaviorRuleType 行为规则类型 1、包含 2、不包含 3、以XX开头 4、以XX结尾 5、正则 6、特殊规则 7、等于
     * @param behaviorRule     行为规则
     * @param dataRuleType     数据规则类型 1：正则 2：Json
     * @param dataRule         数据规则
     * @param weight           权重 高的优先匹配
     * @param des              描述
     * @param active           是否活跃
     * @param addtime          添加时间
     */
    public ItemPattern(int id, String appid, int companyid, int itemTypeId, float starLevel, int behaviorType, int behaviorRuleType, String behaviorRule, int dataRuleType, String dataRule, int weight, String des, int active, long addtime) {
        this.id = id;
        this.appid = appid;
        this.companyid = companyid;
        this.itemTypeId = itemTypeId;
        this.starLevel = starLevel;
        this.behaviorType = behaviorType;
        this.behaviorRuleType = behaviorRuleType;
        this.behaviorRule = behaviorRule;
        this.dataRuleType = dataRuleType;
        this.dataRule = dataRule;
        this.weight = weight;
        this.des = des;
        this.active = active;
        this.addtime = addtime;
        this.init();
    }

    private void init() {
        if (this.behaviorRuleType == 5)
            this.regexPage = java.util.regex.Pattern.compile(this.behaviorRule);

        if (this.dataRuleType == 1)
            this.regexData = java.util.regex.Pattern.compile(this.dataRule);
        else if (this.dataRuleType == 2)
            this.jsonDataPath = this.dataRule.split("\\.");
    }

    /**
     * 匹配行为
     *
     * @param behavior
     * @return
     */
    private boolean matchBehavior(String behavior) {
        //1、包含 2、不包含 3、以XX开头 4、以XX结尾 5、正则 6、特殊规则 7、等于
        try {
            switch (this.behaviorRuleType) {
                case 1:
                    return behavior.contains(this.behaviorRule);
                case 2:
                    return !behavior.contains(this.behaviorRule);
                case 3:
                    return behavior.startsWith(this.behaviorRule);
                case 4:
                    return behavior.endsWith(this.behaviorRule);
                case 5:
                    Matcher matcher = this.regexPage.matcher(behavior);
                    return matcher.find();
                case 6:
                    return false;
                case 7:
                    return behavior.equals(this.behaviorRule);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 匹配物品ID
     *
     * @param data
     * @return
     */
    private String matchItemid(String data) {
        try {
            //1：正则 2：Json
            if (this.dataRuleType == 1) {
                Matcher matcher = this.regexData.matcher(data);
                if (matcher.find() && matcher.groupCount() >= 1) {
                    return matcher.group(1);
                }
            }
            if (this.dataRuleType == 2) {
                JSONObject jsonObject = JSON.parseObject(data);
                final int max = this.jsonDataPath.length;
                for (int i = 0; i < max - 1; i++)
                    jsonObject = jsonObject.getJSONObject(this.jsonDataPath[i]);
                return jsonObject.getString(this.jsonDataPath[max - 1]);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 得到物品
     *
     * @param appid APPID
     * @param page  PAGE
     * @param data  数据
     * @return
     */
    public Item getItem(String appid, String page, String data) {
        if (!this.appid.equals(appid))
            return null;
        if (!matchBehavior(page))
            return null;
        String itemid = matchItemid(data);
        if (itemid == null)
            return null;
        return new Item(this.appid, this.companyid, this.id, this.itemTypeId, itemid, this.starLevel, 0);
    }


    public long getAddtime() {
        return addtime;
    }

    public void setAddtime(long addtime) {
        this.addtime = addtime;
    }

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public static int getBehaviorTypePage() {
        return BEHAVIOR_TYPE_PAGE;
    }

    public static int getBehaviorTypeEvent() {
        return BEHAVIOR_TYPE_EVENT;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
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

    public int getItemTypeId() {
        return itemTypeId;
    }

    public void setItemTypeId(int itemTypeId) {
        this.itemTypeId = itemTypeId;
    }

    public float getStarLevel() {
        return starLevel;
    }

    public void setStarLevel(float starLevel) {
        this.starLevel = starLevel;
    }

    public int getBehaviorType() {
        return behaviorType;
    }

    public void setBehaviorType(int behaviorType) {
        this.behaviorType = behaviorType;
    }

    public int getBehaviorRuleType() {
        return behaviorRuleType;
    }

    public void setBehaviorRuleType(int behaviorRuleType) {
        this.behaviorRuleType = behaviorRuleType;
    }

    public String getBehaviorRule() {
        return behaviorRule;
    }

    public void setBehaviorRule(String behaviorRule) {
        this.behaviorRule = behaviorRule;
    }

    public int getDataRuleType() {
        return dataRuleType;
    }

    public void setDataRuleType(int dataRuleType) {
        this.dataRuleType = dataRuleType;
    }

    public String getDataRule() {
        return dataRule;
    }

    public void setDataRule(String dataRule) {
        this.dataRule = dataRule;
    }

    public int getWeight() {
        return weight;
    }

    public void setWeight(int weight) {
        this.weight = weight;
    }

    public String getDes() {
        return des;
    }

    public void setDes(String des) {
        this.des = des;
    }

    public int getActive() {
        return active;
    }

    public void setActive(int active) {
        this.active = active;
    }
}
