package com.wangjia.bigdata.core.bean.common

import com.google.gson.JsonObject

/**
  * 公共父类
  */
trait ObjectBean extends Serializable {
    def toJson(): JsonObject = {
        new JsonObject()
    }
}