package com.wangjia.bigdata.core.bean.common

import com.google.gson.JsonObject

/**
  * 事件
  *
  * @param eventType 事件大类型
  * @param eventId   事件小类型
  * @param data      数据
  * @param time      时间
  */
case class PageEvent(eventType: String, eventId: String, data: String, time: Long)
        extends ObjectBean {

    override def toJson(): JsonObject = {
        val json = new JsonObject()
        json.addProperty("eventType", eventType)
        json.addProperty("eventId", eventId)
        json.addProperty("data", data)
        json.addProperty("time", time)
        json
    }
}