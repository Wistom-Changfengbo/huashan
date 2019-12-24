package com.wangjia.bigdata.core.bean.common

import com.google.gson.JsonObject

/**
  * 一次页面访问
  *
  * @param pageUrl 页面URL
  * @param title   页面标题
  * @param refUrl  上个页面URL
  * @param start   开始时间
  * @param time    停留时间
  * @param data    页面数据
  */
case class PageReq(pageUrl: String, title: String, refUrl: String, start: Long, time: Long, data: String)
        extends ObjectBean {

    override def toJson(): JsonObject = {
        val json = new JsonObject()
        json.addProperty("pageUrl", pageUrl)
        json.addProperty("title", title)
        json.addProperty("refUrl", refUrl)
        json.addProperty("start", start)
        json.addProperty("time", time)
        json.addProperty("data", data)
        json
    }
}