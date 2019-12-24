package com.wangjia.bean;



import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * 该类用于返回用户根据 手机应用列表 计算得出的一些杂七杂八数据
 * Created by Administrator on 2017/11/6.
 */
public class V2UserProfileRet implements Serializable{
    /**
     * 用户标签与app标签 键值对
     */
    public static final class KeyValue implements Serializable{
        protected String key;
        protected Object value;
        protected Integer isNew;

        public KeyValue() {

        }

        public KeyValue(String key, Object value, Integer isNew) {
            this.key = key;
            this.value = value;
            this.isNew = isNew;
        }

        public KeyValue(String key, Object value) {
            this.key = key;
            this.value = value;
        }

        public Integer getIsNew() {
            return isNew;
        }

        public void setIsNew(Integer isNew) {
            this.isNew = isNew;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public Object getValue() {
            return value;
        }

        public void setValue(Object value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return "KeyValue{" +
                    "key='" + key + '\'' +
                    ", value=" + value +
                    ", isNew=" + isNew +
                    '}';
        }
    }

    /**
     * 消费能力
     */
    public static final class ConsumeAbility implements Serializable{

        public ConsumeAbility() {
            consumeNum = 0;
            consumeDes = "";
        }

        public ConsumeAbility(int consumeNum) {
            this.consumeNum = consumeNum;
            consumeDes = "";
        }


        @JsonProperty("consumenum")
        private Integer consumeNum;
        @JsonProperty("consumedes")
        private String consumeDes;

        public Integer getConsumeNum() {
            return consumeNum;
        }

        public void setConsumeNum(Integer consumeNum) {
            this.consumeNum = consumeNum;
        }

        public String getConsumeDes() {
            return consumeDes;
        }

        public void setConsumeDes(String consumeDes) {
            this.consumeDes = consumeDes;
        }

        @Override
        public String toString() {
            return "Consume{" +
                    "consumeNum=" + consumeNum +
                    ", consumeDes='" + consumeDes + '\'' +
                    '}';
        }
    }

    //性别
    private int sex;

    //活跃度
    private double vitality;

    //用户标签
    private List<KeyValue> userProfile = new LinkedList<>();

    //用户标签
    private List<KeyValue> appTag = new LinkedList<>();

    //活跃星级
    private int vitalityStar;

    //消费水平
    private ConsumeAbility consume;

    //更新时间
    private long    updateTime;

    //平台
    private String  platform;

    //rowKey
    private String rowKey;

    //设备ID
    private String deviceId;

    public int getSex() {
        return sex;
    }

    public void setSex(int sex) {
        this.sex = sex;
    }

    public double getVitality() {
        return vitality;
    }

    public void setVitality(double vitality) {
        this.vitality = vitality;
    }

    public List<KeyValue> getUserProfile() {
        return userProfile;
    }

    public void setUserProfile(List<KeyValue> userProfile) {
        this.userProfile = userProfile;
    }

    public void addUserProfile(String key, Object value) {
        this.userProfile.add(new KeyValue(key, value));
    }

    public int getVitalityStar() {
        return vitalityStar;
    }

    public void setVitalityStar(int vitalityStar) {
        this.vitalityStar = vitalityStar;
    }

    public ConsumeAbility getConsume() {
        return consume;
    }

    public void setConsume(ConsumeAbility consume) {
        this.consume = consume;
    }

    public List<KeyValue> getAppTag() {
        return appTag;
    }

    public void setAppTag(String key, Object value) {
        this.appTag.add(new KeyValue(key, value));
    }

    public long getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(long updateTime) {
        this.updateTime = updateTime;
    }

    public void setAppTag(List<KeyValue> appTag) {
        this.appTag = appTag;
    }

    public String getPlatform() {
        return platform;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }

    public String getrowKey() {
        return rowKey;
    }

    public void setrowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }


    @Override
    public String toString() {
        return "V2UserProfileRet{" +
                "sex=" + sex +
                ", vitality=" + vitality +
                ", userProfile=" + userProfile +
                ", appTag=" + appTag +
                ", vitalityStar=" + vitalityStar +
                ", consume=" + consume +
                ", updateTime=" + updateTime +
                ", platform='" + platform + '\'' +
                ", rowKey='" + rowKey + '\'' +
                ", deviceId='" + deviceId + '\'' +
                '}';
    }

    /**
     *  build 构造对象
     * @param result
     * @return
     */
    public static V2UserProfileRet build(Result result) {
        List<Cell> cs = result.listCells();

        V2UserProfileRet v2UserProfileRet = new V2UserProfileRet();

        JSONObject jsonObject;

        //列名
        String qualifier = "";

        for (Cell cell : cs) {

            //取到修饰名
            qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));

            String rowKey = Bytes.toString(CellUtil.cloneRow(cell));  //取行键
            v2UserProfileRet.rowKey = rowKey;

            switch (qualifier) {
                //app户标签
                case "apptag":
                    String apptagStr = Bytes.toString(CellUtil.cloneValue(cell));
                    jsonObject = JSON.parseObject(apptagStr);
                    for (String key : jsonObject.keySet()) {
                        //app户标签
                        v2UserProfileRet.appTag.add(new KeyValue(key, jsonObject.getIntValue(key)));
                    }
                    break;
                //用户标签
                case "usertag":
                    String usertagStr = Bytes.toString(CellUtil.cloneValue(cell));
                    jsonObject = JSON.parseObject(usertagStr);
                    for (String key : jsonObject.keySet()) {
                        //用户标签
                        v2UserProfileRet.userProfile.add(new KeyValue(key, jsonObject.getIntValue(key)));
                    }
                    break;
                //更新时间
                case "updatetime":
                    long updatetime = Bytes.toLong(CellUtil.cloneValue(cell));
                    v2UserProfileRet.updateTime = updatetime;
                    break;
                //消费水平
                case "consume":
                    int consume = Bytes.toInt(CellUtil.cloneValue(cell));
                    V2UserProfileRet.ConsumeAbility consumeAbility = new V2UserProfileRet.ConsumeAbility(consume);
                    v2UserProfileRet.consume = consumeAbility;
                    break;
                //平台
                case "platform":
                    String platform = Bytes.toString(CellUtil.cloneValue(cell));
                    v2UserProfileRet.platform = platform;
                    break;
                case "sex":
                    double sexDouble = Bytes.toDouble(CellUtil.cloneValue(cell));

                    // 1 男 2女 0 未知
                    int sex = 0;
                    if (sexDouble > 0) {
                        sex = 2;
                    } else if (sexDouble < 0) {
                        sex = 1;
                    }

                    v2UserProfileRet.sex = sex;
                    break;
                default:
                    break;
            }
        }

        return v2UserProfileRet;
    }

}
