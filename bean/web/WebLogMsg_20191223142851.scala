package com.wangjia.bigdata.core.bean.web

import java.util

/**
  * WEB端日志
  *
  * @param ja_version JA版本号
  * @param appid      APPID
  * @param uuid       UUID
  * @param userid     用户ID
  * @param logtype    事件主类型 40：自定义事件 50页面事件
  * @param subtype    事件子类型（当logtype是页面事件50时为1）
  * @param ref        上个页面URL
  * @param url        当前页面URL
  * @param time       服务器时间
  * @param title      标题
  * @param data       数据
  * @param staytime   停留时间
  * @param visitindex 会话下标
  * @param ip         IP
  * @param acc        是否是真人
  * @param deviceInfo 设备信息
  *                   screenWidth  屏幕宽 360
  *                   screenHeight 屏幕高 768
  *                   language     语言  "zh_CN"
  *                   system       系统  "Android 7.1.1"
  *                   browser      浏览器
  *                   agent        浏览器信息
  *                   status       请求状态
  *                   bcookie      是否支持Cookie
  *                   bflash       是否支持Flash
  *                   ja_uuid      JAUUID
  */
case class WebLogMsg(ja_version: String,
                     appid: String,
                     var uuid: String,
                     userid: String,
                     logtype: String,
                     subtype: String,
                     ref: String,
                     url: String,
                     time: Long,
                     title: String,
                     data: String,
                     var staytime: Long,
                     var visitindex: Int,
                     ip: String,
                     var acc: Int,
                     deviceInfo: util.Map[String, Object]) {

    def dStrInfo(key: String): String = {
        if (deviceInfo.containsKey(key)) deviceInfo.get(key).asInstanceOf[String] else ""
    }

    def dStrInfo(key: String, default: String): String = {
        if (deviceInfo.containsKey(key)) deviceInfo.get(key).asInstanceOf[String] else default
    }

    def dIntInfo(key: String): Int = {
        if (deviceInfo.containsKey(key)) deviceInfo.get(key).asInstanceOf[Int] else -1
    }

    def dIntInfo(key: String, default: Int): Int = {
        if (deviceInfo.containsKey(key)) deviceInfo.get(key).asInstanceOf[Int] else default
    }
}