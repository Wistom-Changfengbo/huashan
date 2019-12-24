package com.wangjia.bean;

import com.google.gson.JsonObject;
import com.wangjia.math.ExMath;

/**
 * Created by Administrator on 2017/5/15.
 */
public class Label extends ObjectBean {
    private String id;
    private String name;
    private int day;
    private float value;

    public Label(String id, String name, int day) {
        this.id = id;
        this.name = name;
        this.day = day;
        this.value = 0f;
    }

    public Label(String id, String name, int day, float value) {
        this.id = id;
        this.name = name;
        this.day = day;
        this.value = value;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public float getValue() {
        return value;
    }

    public void setValue(float value) {
        this.value = value;
    }

    public void addValue(int day, float value) {
        int dDay = this.day - day;
        if (dDay > 60 || dDay < 0)
            return;
        this.value += ExMath.attenuation(value,dDay);
    }

    @Override
    public JsonObject toJson() {
        JsonObject obj = new JsonObject();
        obj.addProperty("id", id);
        obj.addProperty("name", name);
        obj.addProperty("value", value);
        return obj;
    }
}
