package com.wangjia.bigdata.core.bean.common

//import com.alibaba.fastjson.JSONObject
import com.google.gson.{JsonArray, JsonObject}
import com.wangjia.bigdata.core.bean.info.IpAddress
import com.wangjia.common.Platform
import net.sf.json.JSONObject

/**
  * Created by Administrator on 2017/5/31.
  */
class UserVisit extends ObjectBean {
    //deviceId
    var deviceId: String = ""
    //平台
    var platform: Int = Platform.PLATFORM_WEB
    //uuid
    var uuid: String = ""
    //appid
    var appid: String = ""
    //ip
    var ip: String = ""
    //来源
    var ref: String = ""
    //地址
    var add: IpAddress = null
    //开始时间
    var start: Long = 0
    //停留时间
    var time: Long = 0
    //版本
    var version: String = "-"
    //网络类型
    var net: String = "-"
    //页面请求集合
    var pReqs: Array[PageReq] = null
    //页面事件
    var pEvents: Array[PageEvent] = null

    override def toJson: JsonObject = {
        val json = new JsonObject()
        json.addProperty("deviceId", deviceId)
        json.addProperty("platform", platform)
        json.addProperty("uuid", uuid)
        json.addProperty("appid", appid)
        json.addProperty("ip", ip)
        json.addProperty("ref", ref)
        if (add != null) {
            json.add("add", add.toJson())
        }
        json.addProperty("start", start)
        json.addProperty("time", time)
        val jpReqs = new JsonArray()
        for (pr <- pReqs) {
            jpReqs.add(pr.toJson())
        }
        json.add("pReqs", jpReqs)
        if (pEvents != null) {
            val jevents = new JsonArray()
            for (e <- pEvents) {
                jevents.add(e.toJson())
            }
            json.add("pEvents", jevents)
        }
        json
    }

    def getEventIds: String = {
        if (pEvents == null || pEvents.length == 0)
            return ""
        val jevents = new JSONObject
        for (e <- pEvents.distinct) {
            jevents.put(e.eventType + "#" + e.eventId, 1)
        }
       ""
    }

    override def toString = {
        toJson.toString
    }
}
