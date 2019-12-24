package com.wangjia.bean;

import java.util.Map;

public class Fengbo {
    private Double sex;
    private Integer consumenum;

    private String[] address;

    private Integer activedaynum;

    private Integer r7activedaynum;

    private Map<String, Object> label_jz;

    public Double getSex() {
        return sex;
    }

    public void setSex(Double sex) {
        this.sex = sex;
    }

    public Integer getConsumenum() {
        return consumenum;
    }

    public void setConsumenum(Integer consumenum) {
        this.consumenum = consumenum == null ? 0 : consumenum / 2;
    }

    public String[] getAddress() {
        return address;
    }

    public void setAddress(String[] address) {
        this.address = address;
    }

    public Integer getActivedaynum() {
        return activedaynum;
    }

    public void setActivedaynum(Integer activedaynum) {
        this.activedaynum = activedaynum;
    }

    public Integer getR7activedaynum() {
        return r7activedaynum;
    }

    public void setR7activedaynum(Integer r7activedaynum) {
        this.r7activedaynum = r7activedaynum;
    }

    public Map<String, Object> getLabel_jz() {
        return label_jz;
    }

    public void setLabel_jz(Map<String, Object> label_jz) {
        this.label_jz = label_jz;
    }
}
