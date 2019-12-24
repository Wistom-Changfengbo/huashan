package com.wangjia.bigdata.core.bean.info

import com.google.gson.JsonObject
import com.wangjia.bigdata.core.bean.common.ObjectBean

/**
  * 地址信息精确到城市
  *
  * @param country  国
  * @param province 省
  * @param city     市
  * @param area     区
  */
case class IpAddress(country: String, province: String, city: String, area: String)
        extends ObjectBean {

    override def toJson(): JsonObject = {
        val json = new JsonObject()
        json.addProperty("country", country)
        json.addProperty("province", province)
        json.addProperty("city", city)
        json.addProperty("area", area)
        json
    }

    override def toString: String = toJson().toString
}

object IpAddress {
    val NULL = new IpAddress("--", "--", "--", "--")

    def apply(adds: Array[String]): IpAddress = {
        if (adds == null || adds.length < 4)
            return NULL
        new IpAddress(adds(0), adds(1), adds(2), adds(3))
    }
}

