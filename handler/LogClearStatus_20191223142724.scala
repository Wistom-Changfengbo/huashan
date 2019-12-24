package com.wangjia.bigdata.core.handler

/**
  * Created by Administrator on 2018/1/16.
  */
object LogClearStatus {
    /**
      * 成功
      */
    val SUCCESS: Int = 200

    /**
      * 格式错误
      */
    val ERROR_FORMAT: Int = 101

    /**
      * JSON简析错误
      */
    val ERROR_JSON: Int = 102

    /**
      * 没有对应的APP标识
      */
    val ERROR_NOT_IDENTIFIER: Int = 103

    /**
      * 没有对应的APPID
      */
    val ERROR_NOT_APPID: Int = 104

    /**
      * 缺少关键字段
      */
    val ERROR_NOT_VITAL_FIELD: Int = 105

    /**
      * 关键字端错误
      */
    val ERROR_VALUE_VITAL_FIELD: Int = 106

    /**
      * 无相关APP
      */
    val ERROR_NOT_APP: Int = 107

    /**
      * 无相关JA版本
      */
    val ERROR_NOT_JA_VERSION: Int = 108

    /**
      * 错误的URL
      */
    val ERROR_URL: Int = 109

    /**
      * 错误的JA_UUID
      */
    val ERROR_JA_UUID: Int = 120

    /**
      * 探针信号不足
      */
    val ERROR_PROBE_LACK_SIGNAL = 121

    /**
      * 错误设备ID
      */
    val ERROR_DEVICEID = 122

    /**
      * 未定意错误
      */
    val ERROR_NOT_KNOWN = 404

}
