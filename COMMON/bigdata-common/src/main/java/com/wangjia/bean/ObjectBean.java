package com.wangjia.bean;

import com.google.gson.JsonObject;

import java.io.Serializable;

/**
 * Created by Administrator on 2017/4/27.
 */
public class ObjectBean implements Serializable {

    public JsonObject toJson(){
        return new JsonObject();
    }
}
