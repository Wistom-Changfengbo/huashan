package com.wangjia.bean;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Created by Cfb on 2018/1/5.
 */
public class EsLabelBean implements Serializable {
    private String uuid;
    private HashMap<String,Integer> labels;

    public EsLabelBean(String uuid, HashMap<String, Integer> labels) {
        this.uuid = uuid;
        this.labels = labels;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public HashMap<String, Integer> getLabels() {
        return labels;
    }

    public void setLabels(HashMap<String, Integer> labels) {
        this.labels = labels;
    }
}
